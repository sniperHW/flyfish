package flykv

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/snap"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"github.com/sniperHW/flyfish/pkg/util"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/server/flypd"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"reflect"
	"sync/atomic"
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
		GetSugar().Errorf("%v", err)
	}
}

func (q applicationQueue) pop() (closed bool, v interface{}) {
	return q.q.Pop()
}

func (q applicationQueue) close() {
	q.q.Close()
}

type clientRequest struct {
	from *fnet.Socket
	msg  *cs.ReqMessage
	slot int
}

type kvmgr struct {
	kv               []map[string]*kv
	slotsKvMap       map[int]map[string]*kv
	slots            *bitmap.Bitmap
	slotsTransferOut map[int]*SlotTransferProposal //正在迁出的slot
	kvcount          int
	lru              lruList
}

type kvstore struct {
	kvmgr
	lastLeader           uint64
	leader               uint64
	snapshotter          *snap.Snapshotter
	rn                   *raft.RaftInstance
	mainQueue            applicationQueue
	db                   dbI
	wait4ReplyCount      int32
	stoped               int32
	ready                int32
	kvnode               *kvnode
	shard                int
	meta                 db.DBMeta
	dbWriteBackCount     int32
	SoftLimitReachedTime int64
	unixNow              int64
}

func (s *kvstore) isLeader() bool {
	return atomic.LoadUint64(&s.leader) == s.rn.ID()
}

func (s *kvstore) isReady() bool {
	return s.isLeader() && atomic.LoadInt32(&s.ready) == 1
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

func (s *kvstore) addCliMessage(req clientRequest) {
	if nil != s.mainQueue.q.Append(0, req) {
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
		})
	}
}

const kvCmdQueueSize = 32

func (s *kvstore) deleteKv(k *kv) {
	s.kvcount--
	delete(s.kv[k.groupID], k.uniKey)
	s.lru.remove(&k.lru)

	kvs := s.slotsKvMap[k.slot]
	delete(kvs, k.uniKey)
	if len(kvs) == 0 {
		delete(s.slotsKvMap, k.slot)
		//当前slot的kv已经全部清除，如果当前slot正在迁出，结束迁出事务
		p := s.slotsTransferOut[k.slot]
		if nil != p {
			delete(s.slotsTransferOut, k.slot)
			s.slots.Set(k.slot)
			if nil != p.reply {
				p.reply()
			}
		}
	}
}

func (s *kvstore) newkv(slot int, groupID int, unikey string, key string, table string) (*kv, errcode.Error) {
	tbmeta := s.meta.GetTableMeta(table)

	if nil == tbmeta {
		return nil, errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", table))
	}

	k := &kv{
		uniKey:  unikey,
		key:     key,
		state:   kv_new,
		meta:    tbmeta,
		store:   s,
		groupID: groupID,
		slot:    slot,
		table:   table,
	}

	k.lru.keyvalue = k

	k.updateTask = dbUpdateTask{
		kv: k,
		state: db.UpdateState{
			Key:    key,
			Slot:   slot,
			Fields: map[string]*flyproto.Field{},
		},
	}

	s.kv[groupID][unikey] = k

	s.kvcount++

	if kvs := s.slotsKvMap[slot]; nil != kvs {
		kvs[unikey] = k
	} else {
		kvs = map[string]*kv{}
		kvs[unikey] = k
		s.slotsKvMap[slot] = kvs
	}

	return k, nil
}

func (this *kvstore) tryKick(kv *kv) bool {
	if !kv.kickable() {
		return false
	} else {
		cmd, _ := this.makeKick(kv, time.Time{}, time.Time{}, nil, 0, nil)
		kv.pushCmd(cmd)
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
	if s.isReady() && atomic.LoadInt32(&s.stoped) == 0 {
		if s.kvcount > s.kvnode.config.MaxCachePerStore && s.lru.head.nnext != &s.lru.tail {
			k := 0
			cur := s.lru.tail.pprev
			for cur != &s.lru.head && s.kvcount-k > s.kvnode.config.MaxCachePerStore {
				if !s.tryKick(cur.keyvalue) {
					return
				} else {
					k++
				}
				cur = cur.pprev
			}
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

func (s *kvstore) checkReqLimit() bool {
	c := int(atomic.LoadInt32(&s.wait4ReplyCount))
	conf := s.kvnode.config.StoreReqLimit

	if c > conf.HardLimit {
		return false
	}

	if c > conf.SoftLimit {
		//nowUnix := s.unixNow
		nowUnix := time.Now().Unix()
		if s.SoftLimitReachedTime == 0 {
			s.SoftLimitReachedTime = nowUnix
		} else {
			elapse := nowUnix - s.SoftLimitReachedTime
			if int(elapse) >= conf.SoftLimitSeconds {
				return false
			}
		}
	} else {
		s.SoftLimitReachedTime = 0
	}

	return true
}

func (s *kvstore) processClientMessage(req clientRequest) {

	resp := &cs.RespMessage{
		Cmd:   req.msg.Cmd,
		Seqno: req.msg.Seqno,
	}

	var (
		err errcode.Error
		cmd cmdI
		kv  *kv
		ok  bool
	)

	err = func() errcode.Error {
		if !s.checkReqLimit() {
			return errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later")
		}

		slot := sslot.Unikey2Slot(req.msg.UniKey)

		if !s.slots.Test(slot) {
			//unikey不归当前store管理,路由信息已经stale
			return errcode.New(errcode.Errcode_route_info_stale)
		}

		if s.slotsTransferOut[slot] != nil {
			//正在迁出
			return errcode.New(errcode.Errcode_slot_transfering)
		}

		if !s.isLeader() {
			return errcode.New(errcode.Errcode_not_leader)
		} else if atomic.LoadInt32(&s.ready) == 0 || s.meta == nil {
			return errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later")
		} else {
			groupID := sslot.StringHash(req.msg.UniKey) % len(s.kv)
			kv, ok = s.kv[groupID][req.msg.UniKey]
			if !ok {
				if req.msg.Cmd == flyproto.CmdType_Kick { //kv不在缓存中,kick操作直接返回ok
					return errcode.New(errcode.Errcode_ok)
				} else {
					if s.kvcount > (s.kvnode.config.MaxCachePerStore*3)/2 {
						return errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later")
					} else {
						table, key := splitUniKey(req.msg.UniKey)
						if kv, err = s.newkv(slot, groupID, req.msg.UniKey, key, table); nil != err {
							return err
						}
					}
				}
			} else if kv.kicking {
				return errcode.New(errcode.Errcode_retry, "please retry later")
			} else {
				tbmeta := s.meta.CheckTableMeta(kv.meta)
				if nil == tbmeta {
					//在最新的meta中kv.table已经被删除
					s.tryKick(kv)
					return errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", kv.table))
				} else {
					kv.meta = tbmeta
				}
			}
		}

		cmd, err = s.makeCmd(kv, req)
		return err
	}()

	if nil == err {
		kv.pushCmd(cmd)
	} else {
		if errcode.GetCode(err) != errcode.Errcode_ok {
			resp.Err = err
		}
		req.from.Send(resp)
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
		s.makeSnapshot(snapshotNotify)
	}
}

func (s *kvstore) processLinearizableRead(r []raft.LinearizableRead) {
	for _, v := range r {
		v.(*kvLinearizableRead).ok()
	}
}

func (s *kvstore) processConfChange(p *ProposalConfChange) {
	p.reply(nil)
}

func (s *kvstore) stop() {
	if atomic.CompareAndSwapInt32(&s.stoped, 0, 1) {
		s.rn.Stop()
	}
}

func (s *kvstore) reportStatus() {
	s.mainQueue.q.Append(1, func() {
		msg := &sproto.StoreReportStatus{
			SetID:       int32(s.kvnode.setID),
			NodeID:      int32(s.kvnode.id),
			StoreID:     int32(s.shard),
			Isleader:    s.isLeader(),
			Kvcount:     int32(s.kvcount),
			Progress:    s.rn.GetApplyIndex(),
			MetaVersion: s.meta.GetVersion(),
			RaftID:      s.rn.ID(),
		}

		go func() {
			for _, v := range s.kvnode.pdAddr {
				s.kvnode.udpConn.SendTo(v, snet.MakeMessage(0, msg))
			}
		}()

		util.OnceTimer(time.Second, s.reportStatus)
	})
}

func (s *kvstore) serve() {

	s.reportStatus()

	go func() {
		ch := make(chan struct{}, 1)
		interval := time.Duration(s.kvnode.config.LruCheckInterval)
		if 0 == interval {
			interval = 1000
		}

		for atomic.LoadInt32(&s.stoped) == 0 {
			time.Sleep(time.Millisecond * interval)
			err := s.mainQueue.q.Append(1, func() {
				s.checkLru(ch)
			})
			if nil == err {
				<-ch
			} else if err == queue.ErrQueueClosed {
				return
			}
		}
	}()

	go func() {
		defer func() {
			s.mainQueue.close()
			s.kvnode.muS.Lock()
			delete(s.kvnode.stores, s.shard)
			s.kvnode.muS.Unlock()
		}()

		for {
			_, v := s.mainQueue.pop()

			switch v.(type) {
			case *udpMsg:
				s.onUdpMsg(v.(*udpMsg).from, v.(*udpMsg).m)
			case raft.TransportError:
				GetSugar().Errorf("%x error for raft transport:%v", s.rn.ID(), v.(raft.TransportError))
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
				s.processConfChange(v.(*ProposalConfChange))
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode {
					if s.rn.ID() == c.NodeID {
						GetSugar().Infof("%x RemoveFromCluster", s.rn.ID())
						s.stop()
					}
				}
			case raft.ReplayOK:
			case raft.RaftStopOK:
				GetSugar().Infof("%x RaftStopOK", s.rn.ID())
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
						if err = s.replayFromBytes(data); err != nil {
							GetSugar().Panic(err)
						}
					}
				}
			case raft.LeaderChange:
				oldLeader := s.leader
				atomic.StoreUint64(&s.leader, uint64(v.(raft.LeaderChange).Leader))
				if v.(raft.LeaderChange).Leader == s.rn.ID() {
					s.becomeLeader()
				}

				if oldLeader == s.rn.ID() && !s.isLeader() {
					s.onLeaderDownToFollower()
				}

			case *SlotTransferProposal:
				if s.isReady() {
					s.processSlotTransferOut(v.(*SlotTransferProposal))
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()
}

func (s *kvstore) issueFullDbWriteBack() {
	GetSugar().Info("WriteBackAll")
	for _, v := range s.kv {
		for _, vv := range v {
			if meta := s.meta.CheckTableMeta(vv.meta); meta != nil {
				vv.meta = meta
				//用最新version作为lastWriteBackVersion
				vv.updateTask.SetLastWriteBackVersion(vv.version)
				vv.updateTask.issueFullDbWriteBack()
			} else {
				s.tryKick(vv)
			}
		}
	}
}

func (s *kvstore) applyNop() {
	atomic.StoreInt32(&s.ready, 1)
	s.issueFullDbWriteBack()
}

func (s *kvstore) becomeLeader() {
	GetSugar().Infof("becomeLeader %v", s.rn.ID())
	s.rn.IssueProposal(&proposalNop{store: s})
}

func (s *kvstore) onLeaderDownToFollower() {
	atomic.StoreInt32(&s.ready, 0)
}

//将nodeID作为learner加入当前store的raft配置
func (s *kvstore) onAddLearnerNode(from *net.UDPAddr, processID uint16, raftID uint64, host string, raftPort int32, port int32, context int64) {
	reply := func(err error) {
		if nil == err || err == membership.ErrIDExists {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context, &sproto.NodeStoreOpOk{}))
		} else {
			switch err {
			case membership.ErrPeerURLexists, membership.ErrTooManyLearners:
				//pd做了控制，不应该出现这些错误
				GetSugar().Errorf("NotifyAddLearner error node:%d store:%d err:%v", s.kvnode.id, s.shard, err)
			default:
				return
			}
		}
	}

	GetSugar().Infof("onAddLearnerNode")

	s.rn.IssueConfChange(&ProposalConfChange{
		confChangeType: raftpb.ConfChangeAddLearnerNode,
		url:            fmt.Sprintf("http://%s:%d", host, raftPort),
		clientUrl:      fmt.Sprintf("http://%s:%d", host, port),
		nodeID:         raftID,
		processID:      processID,
		reply:          reply,
	})
}

//将nodeID提升为当前store的raft配置的voter
func (s *kvstore) onPromoteLearnerNode(from *net.UDPAddr, raftID uint64, context int64) {
	reply := func(err error) {
		if nil == err || err == membership.ErrMemberNotLearner {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context, &sproto.NodeStoreOpOk{}))
		} else {
			switch err {
			case membership.ErrIDNotFound:
				//pd做了控制，不应该出现这些错误
				GetSugar().Errorf("NotifyPromoteLearner error node:%d store:%d err:%v", s.kvnode.id, s.shard, err)
			default:
				return
			}
		}
	}

	GetSugar().Infof("onPromoteLearnerNode")

	if err := s.rn.IsLearnerReady(raftID); nil == err {
		s.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeAddNode,
			isPromote:      true,
			nodeID:         raftID,
			reply:          reply,
		})
	} else if err == membership.ErrIDNotFound {
		//pd做了控制，不应该出现这些错误
		GetSugar().Errorf("NotifyPromoteLearner error node:%d store:%d err:%v", s.kvnode.id, s.shard, err)
	} else if err == raft.ErrLearnerNotReady {
		_, progress := s.rn.GetMemberProgress(raftID)
		GetSugar().Errorf("learner not ready:progress %v", progress)
	}

}

//将nodeID从当前store的raft配置中移除
func (s *kvstore) onRemoveNode(from *net.UDPAddr, raftID uint64, context int64) {

	reply := func(err error) {
		if nil == err || err == membership.ErrIDNotFound {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context, &sproto.NodeStoreOpOk{}))
		}
	}

	if nil == s.rn.MayRemoveMember(types.ID(raftID)) {
		s.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeRemoveNode,
			nodeID:         raftID,
			reply:          reply,
		})
	}

}

func (s *kvstore) onNotifyNodeStoreOp(from *net.UDPAddr, msg *sproto.NotifyNodeStoreOp, context int64) {
	if s.isReady() {
		switch msg.Op {
		case int32(flypd.LearnerStore):
			s.onAddLearnerNode(from, uint16(msg.NodeID), msg.RaftID, msg.Host, msg.RaftPort, msg.Port, context)
		case int32(flypd.VoterStore):
			s.onPromoteLearnerNode(from, msg.RaftID, context)
		case int32(flypd.RemoveStore):
			s.onRemoveNode(from, msg.RaftID, context)
		default:
			GetSugar().Errorf("onNotifyNodeStoreOp invaild Op:%v", msg.Op)
		}
	}
}

func (s *kvstore) onIsTransInReady(from *net.UDPAddr, msg *sproto.IsTransInReady, context int64) {
	s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
		&sproto.IsTransInReadyResp{
			Ready: true,
			Slot:  msg.Slot,
		}))
}

func (s *kvstore) onNotifySlotTransIn(from *net.UDPAddr, msg *sproto.NotifySlotTransIn, context int64) {
	slot := int(msg.Slot)
	if s.slots.Test(slot) {
		s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
			&sproto.SlotTransInOk{
				Slot: msg.Slot,
			}))
	} else {
		s.rn.IssueProposal(&SlotTransferProposal{
			slot:         slot,
			transferType: slotTransferIn,
			store:        s,
			reply: func() {
				s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
					&sproto.SlotTransInOk{
						Slot: msg.Slot,
					}))
			},
		})
	}
}

func (s *kvstore) processSlotTransferOut(p *SlotTransferProposal) {
	kvs := s.slotsKvMap[p.slot]
	if nil != kvs {
		for _, v := range kvs {
			s.tryKick(v)
		}

		p.timer = time.AfterFunc(time.Millisecond*100, func() {
			s.mainQueue.AppendHighestPriotiryItem(p)
		})
	}
}

func (s *kvstore) onNotifySlotTransOut(from *net.UDPAddr, msg *sproto.NotifySlotTransOut, context int64) {
	slot := int(msg.Slot)
	if !s.slots.Test(slot) {
		s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
			&sproto.SlotTransOutOk{
				Slot: msg.Slot,
			}))
	} else {
		p := s.slotsTransferOut[slot]

		reply := func() {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.SlotTransOutOk{
					Slot: msg.Slot,
				}))
		}

		if nil == p {
			p = &SlotTransferProposal{
				slot:         slot,
				transferType: slotTransferOut,
				store:        s,
				reply:        reply,
			}

			s.slotsTransferOut[slot] = p

			s.rn.IssueProposal(p)
		} else {
			//应答最后一个消息
			p.reply = reply
		}
	}
}

func (s *kvstore) onNotifyUpdateMeta(from *net.UDPAddr, msg *sproto.NotifyUpdateMeta, context int64) {
	if s.meta.GetVersion() == msg.Version {
		return
	} else {
		var meta db.DBMeta
		var err error
		var def *db.DbDef
		if def, err = db.MakeDbDefFromJsonString(msg.Meta); nil == err {
			meta, err = sql.CreateDbMeta(def)
		}

		if nil == err {
			s.rn.IssueProposal(&ProposalUpdateMeta{
				meta:  meta,
				store: s,
			})
		} else {
			GetSugar().Infof("onNotifyUpdateMeta ")
		}
	}
}

func (s *kvstore) onUdpMsg(from *net.UDPAddr, m *snet.Message) {
	if s.isReady() && atomic.LoadInt32(&s.stoped) == 0 {
		switch m.Msg.(type) {
		case *sproto.NotifyNodeStoreOp:
			s.onNotifyNodeStoreOp(from, m.Msg.(*sproto.NotifyNodeStoreOp), m.Context)
		case *sproto.NotifySlotTransIn:
			s.onNotifySlotTransIn(from, m.Msg.(*sproto.NotifySlotTransIn), m.Context)
		case *sproto.NotifySlotTransOut:
			s.onNotifySlotTransOut(from, m.Msg.(*sproto.NotifySlotTransOut), m.Context)
		case *sproto.NotifyUpdateMeta:
			s.onNotifyUpdateMeta(from, m.Msg.(*sproto.NotifyUpdateMeta), m.Context)
		case *sproto.IsTransInReady:
			s.onIsTransInReady(from, m.Msg.(*sproto.IsTransInReady), m.Context)
		}
	}
}
