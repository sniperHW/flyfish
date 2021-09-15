package kvnode

import (
	//"errors"

	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type applyable interface {
	apply()
}

func splitUniKey(unikey string) (table string, key string) {
	i := -1
	for k, v := range unikey {
		if v == 58 {
			i = k
			break
		}
	}

	if i >= 0 {
		table = unikey[:i]
		key = unikey[i+1:]
	}

	return
}

type applicationQueue struct {
	q *queue.PriorityQueue
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	if err := q.q.ForceAppend(1, m); nil != err {
		panic(err)
	}
}

func (q applicationQueue) append(m interface{}) {
	q.q.ForceAppend(0, m)
}

func (q applicationQueue) pop() (closed bool, v interface{}) {
	return q.q.Pop()
}

func (q applicationQueue) close() {
	q.q.Close()
}

type clientRequest struct {
	from *conn
	msg  *cs.ReqMessage
	slot int
}

type lruElement struct {
	pprev    *lruElement
	nnext    *lruElement
	keyvalue *kv
}

type lruList struct {
	head lruElement
	tail lruElement
}

func (this *lruList) init() {
	this.head.nnext = &this.tail
	this.tail.pprev = &this.head
}

/*
 * lru每个kv被访问后重新插入列表头部，尾部表示最久未被访问的kv，可以从cache中kick
 */
func (this *lruList) updateLRU(e *lruElement) {
	if e.nnext != nil || e.pprev != nil {
		//先移除
		e.pprev.nnext = e.nnext
		e.nnext.pprev = e.pprev
		e.nnext = nil
		e.pprev = nil
	}

	//插入头部
	e.nnext = this.head.nnext
	e.nnext.pprev = e
	e.pprev = &this.head
	this.head.nnext = e

}

func (this *lruList) removeLRU(e *lruElement) {
	e.pprev.nnext = e.nnext
	e.nnext.pprev = e.pprev
	e.nnext = nil
	e.pprev = nil
}

type kvmgr struct {
	kv    map[string]*kv
	kicks map[string]bool
}

var compressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &compress.ZipCompressor{}
	},
}

func getCompressor() compress.CompressorI {
	return compressorPool.Get().(compress.CompressorI)
}

func releaseCompressor(c compress.CompressorI) {
	compressorPool.Put(c)
}

var uncompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &compress.ZipUnCompressor{}
	},
}

func getUnCompressor() compress.UnCompressorI {
	return uncompressorPool.Get().(compress.UnCompressorI)
}

func releaseUnCompressor(c compress.UnCompressorI) {
	uncompressorPool.Put(c)
}

type kvstore struct {
	raftMtx          sync.Mutex
	raftID           int
	leader           int
	snapshotter      *snap.Snapshotter
	rn               *raft.RaftNode
	mainQueue        applicationQueue
	keyvals          []kvmgr
	db               dbbackendI
	lru              lruList
	wait4ReplyCount  int32
	lease            *lease
	stoponce         int32
	ready            bool
	kvnode           *kvnode
	needWriteBackAll bool
	shard            int
	slots            *bitmap.Bitmap
	meta             db.DBMeta
	removeonce       int32
	removing         bool
}

func (s *kvstore) hasLease() bool {
	r := s.lease.hasLease()
	if !r {
		s.needWriteBackAll = true
	}
	return r
}

func (s *kvstore) isLeader() bool {
	s.raftMtx.Lock()
	s.raftMtx.Unlock()
	return s.leader == s.raftID
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvstore) addCliMessage(msg clientRequest) {
	s.mainQueue.append(msg)
}

const kvCmdQueueSize = 32

func (s *kvstore) newkv(groupID int, unikey string, key string, table string) (*kv, errcode.Error) {
	tbmeta := s.meta.GetTableMeta(table)

	if nil == tbmeta {
		return nil, errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", table))
	}

	kv := &kv{
		uniKey:  unikey,
		key:     key,
		state:   kv_new,
		tbmeta:  tbmeta,
		store:   s,
		groupID: groupID,
	}
	kv.lru.keyvalue = kv
	kv.updateTask = dbUpdateTask{
		keyValue:     kv,
		updateFields: map[string]*flyproto.Field{},
	}
	return kv, nil
}

func (this *kvstore) tryKick(kv *kv) bool {
	if kv.kickable() {
		kv.kicking = true
	} else {
		return false
	}

	proposal := &kvProposal{
		ptype:    proposal_kick,
		keyValue: kv,
	}

	if err := this.rn.IssueProposal(proposal); nil != err {
		kv.kicking = false
		return false
	} else {
		return true
	}
}

func (s *kvstore) checkLru(ch chan struct{}) {
	defer func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}()

	if s.lru.head.nnext != &s.lru.tail {
		cur := s.lru.tail.pprev
		for cur != &s.lru.head && len(s.keyvals) > s.kvnode.config.MaxCachePerStore {
			if !s.tryKick(cur.keyvalue) {
				return
			}
			cur = cur.pprev
		}
	}
}

func getDeadline(timeout uint32) (time.Time, time.Time) {
	now := time.Now()
	t := time.Duration(timeout) * time.Millisecond
	processDeadline := now.Add(t / 2)
	respDeadline := now.Add(t)
	return processDeadline, respDeadline
}

func (s *kvstore) makeCmd(keyvalue *kv, req clientRequest) (cmdI, errcode.Error) {
	cmd := req.msg.Cmd
	data := req.msg.Data
	processDeadline, respDeadline := getDeadline(req.msg.Timeout)
	switch cmd {
	case flyproto.CmdType_Get:
		return s.makeGet(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.GetReq))
	case flyproto.CmdType_Set:
		return s.makeSet(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.SetReq))
	case flyproto.CmdType_SetNx:
		return s.makeSetNx(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.SetNxReq))
	case flyproto.CmdType_Del:
		return s.makeDel(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.DelReq))
	case flyproto.CmdType_CompareAndSet:
		return s.makeCompareAndSet(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.CompareAndSetReq))
	case flyproto.CmdType_CompareAndSetNx:
		return s.makeCompareAndSetNx(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.CompareAndSetNxReq))
	case flyproto.CmdType_IncrBy:
		return s.makeIncr(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.IncrByReq))
	case flyproto.CmdType_DecrBy:
		return s.makeDecr(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.DecrByReq))
	case flyproto.CmdType_Kick:
		return s.makeKick(keyvalue, processDeadline, respDeadline, req.from, req.msg.Seqno, data.(*flyproto.KickReq))
	default:
	}
	return nil, errcode.New(errcode.Errcode_error, "invaild cmd type")
}

func (s *kvstore) processClientMessage(req clientRequest) {

	if atomic.LoadInt32(&s.wait4ReplyCount) >= int32(s.kvnode.config.MainQueueMaxSize) {
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
		})
		return
	}

	slot := sslot.Unikey2Slot(req.msg.UniKey)

	if !s.slots.Test(slot) {
		//unikey不归当前store管理
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, fmt.Sprintf("%s not in current server", req.msg.UniKey)),
		})
		return
	}

	if s.removing {
		//store正被移除
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, fmt.Sprintf("%s current store is removing", req.msg.UniKey)),
		})
	} else if s.leader != s.raftID {
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_not_leader, ""),
		})
	} else if !s.ready || s.meta == nil {
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later"),
		})
	} else {

		var (
			err      errcode.Error
			cmd      cmdI
			keyvalue *kv
			ok       bool
		)

		groupID := sslot.StringHash(req.msg.UniKey) % len(s.keyvals)

		keyvalue, ok = s.keyvals[groupID].kv[req.msg.UniKey]

		if !ok {
			if req.msg.Cmd == flyproto.CmdType_Kick { //kv不在缓存中,kick操作直接返回ok
				req.from.send(&cs.RespMessage{
					Seqno: req.msg.Seqno,
					Cmd:   req.msg.Cmd,
				})
				return
			} else {
				if len(s.keyvals) > (s.kvnode.config.MaxCachePerStore*3)/2 {
					req.from.send(&cs.RespMessage{
						Cmd:   req.msg.Cmd,
						Seqno: req.msg.Seqno,
						Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
					})
					GetSugar().Infof("reply retry %d %d", len(s.keyvals), s.kvnode.config.MaxCachePerStore)
					return
				} else {
					table, key := splitUniKey(req.msg.UniKey)
					keyvalue, err = s.newkv(groupID, req.msg.UniKey, key, table)
					if nil != err {
						req.from.send(&cs.RespMessage{
							Cmd:   req.msg.Cmd,
							Seqno: req.msg.Seqno,
							Err:   err,
						})
						return
					} else {
						s.keyvals[keyvalue.groupID].kv[req.msg.UniKey] = keyvalue
					}
				}
			}
		}

		cmd, err = s.makeCmd(keyvalue, req)
		if nil != err {
			req.from.send(&cs.RespMessage{
				Cmd:   req.msg.Cmd,
				Seqno: req.msg.Seqno,
				Err:   err,
			})
		} else {
			keyvalue.process(cmd)
		}
	}
}

func (s *kvstore) processCommited(commited *raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply()
		}
	} else {
		err := s.replayFromBytes(false, commited.Data)
		if nil != err {
			GetSugar().Panic(err)
		}
	}

	//raft请求snapshot,建立snapshot并返回
	snapshotNotify := commited.GetSnapshotNotify()
	if nil != snapshotNotify {
		snapshot, _ := s.getSnapshot()
		snapshotNotify.Notify(snapshot)
	}
}

func (s *kvstore) processLinearizableRead(r []raft.LinearizableRead) {
	for _, v := range r {
		v.(*kvLinearizableRead).ok()
	}
}

func (s *kvstore) processConfChange(p raft.ProposalConfChange) {

}

func (s *kvstore) stop() {
	if atomic.CompareAndSwapInt32(&s.stoponce, 0, 1) {
		s.lease.stop()
		s.rn.Stop()
	}
}

func (s *kvstore) gotLease() {
	if s.needWriteBackAll {
		GetSugar().Info("WriteBackAll")
		s.needWriteBackAll = false
		for _, v := range s.keyvals {
			for _, vv := range v.kv {
				if vv.tbmeta.GetVersion() != s.meta.GetVersion() {
					vv.tbmeta = s.meta.GetTableMeta(vv.tbmeta.TableName())
				}
				err := vv.updateTask.issueFullDbWriteBack()
				if nil != err {
					break
				}
			}
		}
	}
}

type TestConfChange struct {
	raft.ProposalConfChangeBase
	ch chan error
}

func (this TestConfChange) GetType() raftpb.ConfChangeType {
	return this.ConfChangeType
}

func (this TestConfChange) GetUrl() string {
	return this.Url
}

func (this TestConfChange) GetNodeID() uint64 {
	return this.NodeID
}

func (this TestConfChange) OnError(err error) {

}

func (s *kvstore) addNode(id uint64, url string) {
	o := TestConfChange{
		ProposalConfChangeBase: raft.ProposalConfChangeBase{
			ConfChangeType: raftpb.ConfChangeAddNode,
			Url:            url,
			NodeID:         id,
		},
	}

	s.rn.IssueConfChange(o)
}

func (s *kvstore) serve() {

	go func() {
		ch := make(chan struct{}, 1)
		interval := time.Duration(s.kvnode.config.LruCheckInterval)
		if 0 == interval {
			interval = 1000
		}

		for {
			time.Sleep(time.Millisecond * interval)
			if nil != s.mainQueue.q.Append(0, func() {
				s.checkLru(ch)
			}) {
				return
			}
			<-ch
		}
	}()

	go func() {

		defer func() {
			s.stop()
			s.kvnode.muS.Lock()
			delete(s.kvnode.stores, s.shard)
			s.kvnode.muS.Unlock()
		}()

		for {
			closed, v := s.mainQueue.pop()
			if closed {
				GetSugar().Info("mainQueue stop")
				return
			} else {
				switch v.(type) {
				case error:
					GetSugar().Errorf("error for raft:%v", v.(error))
					return
				case func():
					v.(func())()
				case clientRequest:
					s.processClientMessage(v.(clientRequest))
				case raft.Committed:
					c := v.(raft.Committed)
					s.processCommited(&c)
				case []raft.LinearizableRead:
					s.processLinearizableRead(v.([]raft.LinearizableRead))
				case raft.ProposalConfChange:
					s.processConfChange(v.(raft.ProposalConfChange))
				case raft.RemoveFromCluster:
					GetSugar().Info("RemoveFromCluster")
					return
				case raft.ReplayOK:
					s.ready = true
				case raft.RaftStopOK:
					GetSugar().Info("RaftStopOK")
					return
				case raftpb.Snapshot:
					snapshot := v.(raftpb.Snapshot)

					GetSugar().Infof("%x loading snapshot at term %d and index %d", s.rn.ID(), snapshot.Metadata.Term, snapshot.Metadata.Index)
					r := newSnapshotReader(snapshot.Data)
					var data []byte
					var isOver bool
					var err error
					for {
						isOver, data, err = r.read()
						if isOver {
							break
						} else if nil != err {
							GetSugar().Panic(err)
						} else {
							if err = s.replayFromBytes(true, data); err != nil {
								GetSugar().Panic(err)
							}
						}
					}
				case raft.LeaderChange:
					becomeLeader := false
					s.raftMtx.Lock()
					s.leader = v.(raft.LeaderChange).Leader
					if s.leader == s.raftID {
						becomeLeader = true
					}
					s.raftMtx.Unlock()
					if becomeLeader {
						s.needWriteBackAll = true
						s.lease.becomeLeader()
					}
				default:
					GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
				}
			}
		}
	}()
}
