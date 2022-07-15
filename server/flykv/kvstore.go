package flykv

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/snap"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/list"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
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
	replyer *replyer
	msg     *cs.ReqMessage
	slot    int
}

type slot struct {
	kvMap map[string]*kv
}

type slotMgr struct {
	slots                 []*slot
	kvcount               int
	hardkvlimited         int
	kickableList          *list.List
	slotsTransferOut      map[int]bool //标记迁出的slot
	slotsTransferOutTimer *time.Timer
}

type kvstore struct {
	slotMgr
	leader           uint64
	snapshotter      *snap.Snapshotter
	rn               *raft.RaftInstance
	mainQueue        applicationQueue
	db               dbI
	stoped           int32
	ready            int32
	kvnode           *kvnode
	shard            int
	meta             db.DBMeta
	dbWriteBackCount int32
	halt             bool //停机状态，不处理任何客户端消息
	term             int64
}

func (s *kvstore) addKickable(k *kv) {
	if !k.kickable() {
		s.kickableList.PushBack(&k.listElement)
	}
}

func (s *kvstore) removeKickable(k *kv) {
	if k.kickable() {
		s.kickableList.Remove(&k.listElement)
	}
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
		req.replyer.reply(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
		})
	}
}

func (s *kvstore) deleteKv(k *kv) {
	k.clearCmds(errcode.New(errcode.Errcode_retry, "please try again"))
	s.kvcount--
	delete(s.slots[k.slot].kvMap, k.uniKey)
}

func (s *kvstore) getkv(slot int, unikey string) *kv {
	return s.slots[slot].kvMap[unikey]
}

func (s *kvstore) newkv(slot int, unikey string, key string, table string) (*kv, errcode.Error) {
	tbmeta := s.meta.GetTableMeta(table)

	if nil == tbmeta {
		return nil, errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", table))
	}

	GetSugar().Debugf("newkv:%s", unikey)

	k := &kv{
		uniKey:     unikey,
		key:        key,
		state:      kv_new,
		meta:       tbmeta,
		store:      s,
		slot:       slot,
		table:      table,
		pendingCmd: list.New(),
	}
	k.listElement.Value = k
	k.updateTask = dbUpdateTask{
		kv: k,
		state: db.UpdateState{
			Key:    key,
			Slot:   slot,
			Fields: map[string]*flyproto.Field{},
		},
	}

	s.slots[slot].kvMap[k.uniKey] = k
	s.kvcount++

	return k, nil
}

func (s *kvstore) kick(kv *kv) {
	kick := &cmdKick{}
	kick.cmdBase.init(kick, kv, nil, time.Time{}, nil)
	kv.pushCmd(kick)
}

func (s *kvstore) makeCmd(keyvalue *kv, req clientRequest) (cmdI, errcode.Error) {
	cmd := req.msg.Cmd
	data := req.msg.Data
	deadline := time.Now().Add(time.Duration(req.msg.Timeout) * time.Millisecond)
	switch cmd {
	case flyproto.CmdType_Get:
		return s.makeGet(keyvalue, deadline, req.replyer, data.(*flyproto.GetReq))
	case flyproto.CmdType_Set:
		return s.makeSet(keyvalue, deadline, req.replyer, data.(*flyproto.SetReq))
	case flyproto.CmdType_SetNx:
		return s.makeSetNx(keyvalue, deadline, req.replyer, data.(*flyproto.SetNxReq))
	case flyproto.CmdType_Del:
		return s.makeDel(keyvalue, deadline, req.replyer, data.(*flyproto.DelReq))
	case flyproto.CmdType_CompareAndSet:
		return s.makeCompareAndSet(keyvalue, deadline, req.replyer, data.(*flyproto.CompareAndSetReq))
	case flyproto.CmdType_CompareAndSetNx:
		return s.makeCompareAndSetNx(keyvalue, deadline, req.replyer, data.(*flyproto.CompareAndSetNxReq))
	case flyproto.CmdType_IncrBy:
		return s.makeIncr(keyvalue, deadline, req.replyer, data.(*flyproto.IncrByReq))
	case flyproto.CmdType_Kick:
		return s.makeKick(keyvalue, deadline, req.replyer, data.(*flyproto.KickReq))
	default:
	}
	return nil, errcode.New(errcode.Errcode_error, "invaild cmd type")
}

func (s *kvstore) processClientMessage(req clientRequest) {

	if s.halt {
		req.replyer.dropReply()
		return
	}

	resp := &cs.RespMessage{
		Cmd: req.msg.Cmd,
	}

	var (
		err errcode.Error
		cmd cmdI
		k   *kv
	)

	err = func() errcode.Error {
		slot := sslot.Unikey2Slot(req.msg.UniKey)

		if nil == s.slots[slot] {
			//unikey不归当前store管理,路由信息已经stale
			return errcode.New(errcode.Errcode_route_info_stale)
		}

		if s.slotsTransferOut[slot] {
			//正在迁出
			return errcode.New(errcode.Errcode_slot_transfering)
		}

		if !s.isLeader() {
			return errcode.New(errcode.Errcode_not_leader)
		} else if atomic.LoadInt32(&s.ready) == 0 || s.meta == nil {
			return errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later")
		} else {
			k = s.getkv(slot, req.msg.UniKey)
			if nil == k {
				if req.msg.Cmd == flyproto.CmdType_Kick { //kv不在缓存中,kick操作直接返回ok
					return errcode.New(errcode.Errcode_ok)
				} else {
					if s.kvcount > s.kvnode.config.MaxCachePerStore && nil != s.kickableList.Front() {
						s.kick(s.kickableList.Front().Value.(*kv))
					}

					if s.kvcount > s.hardkvlimited {
						return errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later")
					} else {
						table, key := splitUniKey(req.msg.UniKey)
						if k, err = s.newkv(slot, req.msg.UniKey, key, table); nil != err {
							return err
						}
					}
				}
			} else {
				tbmeta := s.meta.CheckTableMeta(k.meta)
				if nil == tbmeta {
					//在最新的meta中kv.table已经被删除
					return errcode.New(errcode.Errcode_error, fmt.Sprintf("table:%s no define", k.table))
				} else {
					k.meta = tbmeta
				}
			}
		}

		cmd, err = s.makeCmd(k, req)
		return err
	}()

	if nil == err {
		k.pushCmd(cmd)
	} else {
		if errcode.GetCode(err) != errcode.Errcode_ok {
			resp.Err = err
		}
		req.replyer.reply(resp)
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
		go func() {
			for {
				if s.rn.Snapshotting() {
					time.Sleep(time.Second)
				} else {
					break
				}
			}
			s.rn.Stop()
		}()
	}
}

func (s *kvstore) getTerm() int64 {
	return atomic.LoadInt64(&s.term)
}

func (s *kvstore) serve() {

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
				GetSugar().Infof("%x RaftStopOK,kvcount:%d", s.rn.ID(), s.kvcount)
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
				atomic.AddInt64(&s.term, 1)
				oldLeader := s.leader
				atomic.StoreUint64(&s.leader, uint64(v.(raft.LeaderChange).Leader))
				if v.(raft.LeaderChange).Leader == s.rn.ID() {
					s.becomeLeader()
				}

				if oldLeader == s.rn.ID() && !s.isLeader() {
					s.onLeaderDownToFollower()
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()
}

func (s *kvstore) issueFullDbWriteBack() {
	writebackcount := 0
	for _, v := range s.slots {
		if nil != v {
			for _, vv := range v.kvMap {
				if meta := s.meta.CheckTableMeta(vv.meta); meta != nil {
					vv.meta = meta
				}
				vv.updateTask.setLastWriteBackVersion(vv.lastWriteBackVersion)
				if s.kvnode.writeBackMode == write_through && vv.lastWriteBackVersion != vv.version {
					GetSugar().Infof("issueFullDbWriteBack %s %d %d", vv.uniKey, vv.lastWriteBackVersion, vv.version)
					writebackcount++
					vv.updateTask.issueFullDbWriteBack()
				}
				s.addKickable(vv)
			}
		}
	}
	GetSugar().Infof("WriteBackAll kv:%d kvcount:%d", writebackcount, s.kvcount)
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
	for _, v := range s.slots {
		if nil != v {
			for _, vv := range v.kvMap {
				vv.clearCmds(errcode.New(errcode.Errcode_not_leader))
				if vv.state < kv_ok {
					s.deleteKv(vv)
				} else {
					s.removeKickable(vv)
				}
			}
		}
	}
	s.slotsTransferOut = map[int]bool{}
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

	GetSugar().Infof("onAddLearnerNode set:%d kvnode:%d store:%d", s.kvnode.setID, processID, s.shard)

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

	if err := s.rn.MayRemoveMember(types.ID(raftID)); nil == err {
		s.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeRemoveNode,
			nodeID:         raftID,
			reply:          reply,
		})
	} else {
		reply(err)
	}
}

func (s *kvstore) onNotifyNodeStoreOp(from *net.UDPAddr, msg *sproto.NotifyNodeStoreOp, context int64) {
	if s.isReady() {
		switch sproto.StoreOpType(msg.Op) {
		case sproto.StoreOpType_AddLearner:
			s.onAddLearnerNode(from, uint16(msg.NodeID), msg.RaftID, msg.Host, msg.RaftPort, msg.Port, context)
		case sproto.StoreOpType_PromoteLearner:
			s.onPromoteLearnerNode(from, msg.RaftID, context)
		case sproto.StoreOpType_RemoveStore:
			s.onRemoveNode(from, msg.RaftID, context)
		default:
			GetSugar().Errorf("onNotifyNodeStoreOp invaild Op:%v", msg.Op)
		}
	}
}

func (s *kvstore) onIsTransInReady(from *net.UDPAddr, msg *sproto.IsTransInReady, context int64) {
	s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
		&sproto.IsTransInReadyResp{
			Ready: !s.slotsTransferOut[int(msg.Slot)],
			Slot:  msg.Slot,
		}))
}

func (s *kvstore) onNotifySlotTransIn(from *net.UDPAddr, msg *sproto.NotifySlotTransIn, context int64) {
	slot := int(msg.Slot)
	if s.slotsTransferOut[slot] {
		return
	} else if nil != s.slots[slot] {
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

func (s *kvstore) kickSlot() {
	s.slotsTransferOutTimer = nil
	if s.isReady() && len(s.slotsTransferOut) > 0 {
		active := 0
		for slot, _ := range s.slotsTransferOut {
			if kvs := s.slots[slot].kvMap; len(kvs) > 0 {
				active++
				for _, v := range kvs {
					if v.kickable() {
						//只对处于kickable list的kv执行kick
						s.kick(v)
					}
				}
				GetSugar().Infof("processSlotTransferOut slot:%d kick kvcount:%d", slot, len(kvs))
			}
		}

		if active > 0 {
			s.slotsTransferOutTimer = time.AfterFunc(time.Millisecond*100, func() {
				s.mainQueue.AppendHighestPriotiryItem(s.kickSlot)
			})
		}
	}
}

func (s *kvstore) onNotifySlotTransOut(from *net.UDPAddr, msg *sproto.NotifySlotTransOut, context int64) {
	slot := int(msg.Slot)
	if nil == s.slots[slot] {
		s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
			&sproto.SlotTransOutOk{
				Slot: msg.Slot,
			}))
	} else {
		s.slotsTransferOut[slot] = true
		kvs := s.slots[slot].kvMap
		if 0 == len(kvs) {
			s.rn.IssueProposal(&SlotTransferProposal{
				slot:         slot,
				transferType: slotTransferOut,
				store:        s,
				reply: func() {
					s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
						&sproto.SlotTransOutOk{
							Slot: msg.Slot,
						}))
				},
			})
		} else if nil == s.slotsTransferOutTimer {
			s.kickSlot()
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
			GetSugar().Infof("onNotifyUpdateMeta err:%v", err)
		}
	}
}

func (s *kvstore) clearCache() {
	for v := s.kickableList.Front(); nil != v; v = s.kickableList.Front() {
		s.kick(v.Value.(*kv))
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
		case *sproto.ClearStoreCache:
			s.clearCache()
		case *sproto.SuspendStore:
			s.rn.IssueProposal(&SuspendProposal{
				store: s,
			})
		case *sproto.ResumeStore:
			s.rn.IssueProposal(&ResumeProposal{
				store: s,
			})
		case *sproto.TrasnferLeader:
			s.rn.TransferLeadership(m.Msg.(*sproto.TrasnferLeader).Transferee)
		}
	}
}
