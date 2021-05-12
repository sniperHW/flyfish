package kvnode

import (
	//"errors"
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"reflect"
	"runtime"
	"sync"
	"time"
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

func (q applicationQueue) append(m interface{}) error {
	return q.q.Append(0, m)
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

type kvstore struct {
	raftMtx            sync.Mutex
	raftID             int
	leader             int
	snapshotter        *snap.Snapshotter
	rn                 *raft.RaftNode
	mainQueue          applicationQueue
	keyvals            map[string]*kv
	db                 dbbackendI
	lru                lruList
	wait4ReplyCount    int32
	proposalCompressor compress.CompressorI
	snapCompressor     compress.CompressorI
	unCompressor       compress.UnCompressorI
	lease              *lease
	stoponce           sync.Once
	ready              bool
	kvnode             *kvnode
	needWriteBackAll   bool
	shard              int
	slots              *bitmap.Bitmap
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

func (s *kvstore) addCliMessage(msg clientRequest) error {
	return s.mainQueue.append(msg)
}

func (s *kvstore) newkv(unikey string, key string, table string) (*kv, errcode.Error) {
	tbmeta := s.db.getTableMeta(table)

	if nil == tbmeta {
		return nil, errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", table))
	}

	kv := &kv{
		uniKey:     unikey,
		key:        key,
		state:      kv_new,
		tbmeta:     tbmeta,
		pendingCmd: newCmdQueue(64),
		store:      s,
	}
	kv.lru.keyvalue = kv
	kv.updateTask = dbUpdateTask{
		keyValue: kv,
	}
	kv.loadTask = dbLoadTask{
		keyValue: kv,
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
		for cur != &s.lru.head && len(s.keyvals) > GetConfig().MaxCachePerStoreSize {
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

	if !s.ready {
		req.from.send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later"),
		})
		GetSugar().Infof("reply retry")
	} else {

		var (
			err      errcode.Error
			cmd      cmdI
			keyvalue *kv
			ok       bool
		)

		keyvalue, ok = s.keyvals[req.msg.UniKey]

		if !ok {
			if req.msg.Cmd == flyproto.CmdType_Kick { //kv不在缓存中,kick操作直接返回ok
				req.from.send(&cs.RespMessage{
					Seqno: req.msg.Seqno,
					Cmd:   req.msg.Cmd,
				})
				return
			} else {
				if len(s.keyvals) > (GetConfig().MaxCachePerStoreSize*3)/2 {
					req.from.send(&cs.RespMessage{
						Cmd:   req.msg.Cmd,
						Seqno: req.msg.Seqno,
						Err:   errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later"),
					})
					GetSugar().Infof("reply retry")
					return
				} else {
					table, key := splitUniKey(req.msg.UniKey)
					keyvalue, err = s.newkv(req.msg.UniKey, key, table)
					if nil != err {
						req.from.send(&cs.RespMessage{
							Cmd:   req.msg.Cmd,
							Seqno: req.msg.Seqno,
							Err:   err,
						})
						return
					} else {
						s.keyvals[req.msg.UniKey] = keyvalue
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
		err := s.replayFromBytes(commited.Data)
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

func (s *kvstore) replayFromBytes(b []byte) error {

	var err error

	GetSugar().Debugf("replayFromBytes len:%d", len(b))

	compress := b[len(b)-1]
	b = b[:len(b)-1]
	if compress == byte(1) {
		b, err = s.unCompressor.UnCompress(b)
		if nil != err {
			GetSugar().Errorf("UnCompress error %v", err)
			return err
		}
	}

	r := newProposalReader(b)

	var ptype proposalType
	var data interface{}
	var isOver bool
	for {
		isOver, ptype, data, err = r.read()
		if nil != err {
			return err
		} else if isOver {
			return nil
		}

		if ptype == proposal_lease {
			p := data.(pplease)
			s.lease.update(p.nodeid, p.begtime)
		} else {
			p := data.(ppkv)
			keyvalue, ok := s.keyvals[p.unikey]
			GetSugar().Debugf("%s %d %d", p.unikey, ptype, p.version)
			if !ok {
				if ptype == proposal_kick || ptype == proposal_update {
					return fmt.Errorf("bad data,%s with a bad proposal_type:%d", p.unikey, ptype)
				} else {
					var e errcode.Error
					table, key := splitUniKey(p.unikey)
					keyvalue, e = s.newkv(p.unikey, key, table)
					if nil != e {
						return fmt.Errorf("bad data,%s is no table define", p.unikey)
					}
					s.keyvals[p.unikey] = keyvalue
				}
			}

			switch ptype {
			case proposal_kick:
				delete(s.keyvals, p.unikey)
			case proposal_update:
				keyvalue.version = p.version
				for k, v := range p.fields {
					keyvalue.fields[k] = v
				}
			case proposal_snapshot:
				keyvalue.version = p.version
				keyvalue.fields = p.fields
				if keyvalue.version != 0 {
					keyvalue.state = kv_ok
				} else {
					keyvalue.state = kv_no_record
				}
			}
			GetSugar().Debugf("%s ok", p.unikey)
		}
	}

	return nil
}

func (s *kvstore) getSnapshot() ([]byte, error) {

	var groupSize int = GetConfig().SnapshotCurrentCount

	if 0 == groupSize {
		groupSize = runtime.NumCPU()
	}

	beg := time.Now()

	kvGroup := make([][]*kv, groupSize, groupSize)
	i := 0

	for _, v := range s.keyvals {
		if v.state == kv_ok || v.state == kv_no_record {
			kvGroup[i] = append(kvGroup[i], v)
			i = (i + 1) % len(kvGroup)
		}
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(groupSize)

	buff := make([]byte, 0, 1024*64)
	var mtx sync.Mutex

	//多线程序列化和压缩
	for i, _ := range kvGroup {
		go func(i int) {
			b := make([]byte, 0, 1024*64)
			b = buffer.AppendInt32(b, 0) //占位符
			for _, v := range kvGroup[i] {
				b = serilizeKv(b, proposal_snapshot, v.uniKey, v.version, v.fields)
			}

			compressor := s.snapCompressor.Clone()

			cb, err := compressor.Compress(b[4:])
			if nil != err {
				GetSugar().Errorf("snapshot compress error:%v", err)
				b = append(b, byte(0))
				binary.BigEndian.PutUint32(b[:4], uint32(len(b)-4))
			} else {
				b = make([]byte, 0, len(cb)+4+1)
				b = buffer.AppendInt32(b, int32(len(cb)+1))
				b = append(b, cb...)
				b = append(b, byte(1))
			}

			mtx.Lock()
			buff = append(buff, b...)
			mtx.Unlock()

			waitGroup.Done()
		}(i)
	}

	waitGroup.Wait()

	buff = s.lease.snapshot(buff)

	GetSugar().Infof("getSnapshot: %v,snapshot len:%d", time.Now().Sub(beg), len(buff))

	return buff, nil
}

func (s *kvstore) stop() {
	s.stoponce.Do(func() {
		s.rn.Stop()
	})
}

func (s *kvstore) gotLease() {
	if s.needWriteBackAll {
		GetSugar().Info("WriteBackAll")
		s.needWriteBackAll = false
		for _, v := range s.keyvals {
			err := v.updateTask.issueFullDbWriteBack()
			if nil != err {
				break
			}
		}
	}
}

type snapshotReader struct {
	reader buffer.BufferReader
}

func newSnapshotReader(b []byte) *snapshotReader {
	return &snapshotReader{
		reader: buffer.NewReader(b),
	}
}

func (this *snapshotReader) read() (isOver bool, data []byte, err error) {
	if this.reader.IsOver() {
		isOver = true
		return
	} else {
		var l int32
		l, err = this.reader.CheckGetInt32()
		if nil != err {
			return
		}
		data, err = this.reader.CheckGetBytes(int(l))
		if nil != err {
			return
		}
		return
	}
}

func (s *kvstore) serve() {

	go func() {
		ch := make(chan struct{}, 1)
		interval := time.Duration(GetConfig().LruCheckInterval)
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
				case raft.ReplaySnapshot:
					snapshot, err := s.loadSnapshot()
					if err != nil {
						GetSugar().Panic(err)
					}
					if snapshot != nil {
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
								if err = s.replayFromBytes(data); err != nil {
									GetSugar().Panic(err)
								}
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
