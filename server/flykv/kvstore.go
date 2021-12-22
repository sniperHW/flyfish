package flykv

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/snap"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	proposal_none          = proposalType(0)
	proposal_snapshot      = proposalType(1) //全量数据kv快照,
	proposal_update        = proposalType(2) //fields变更
	proposal_kick          = proposalType(3) //从缓存移除kv
	proposal_lease         = proposalType(4) //数据库update权租约
	proposal_slots         = proposalType(5)
	proposal_slot_transfer = proposalType(6)
	proposal_meta          = proposalType(7)
)

type proposalBase struct {
}

func (this *proposalBase) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		c := getCompressor()
		cb, err := c.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
		releaseCompressor(c)
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
}

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
	raftMtx              sync.RWMutex
	leader               raft.RaftInstanceID
	snapshotter          *snap.Snapshotter
	rn                   *raft.RaftInstance
	mainQueue            applicationQueue
	db                   dbI
	wait4ReplyCount      int32
	lease                *lease
	stoped               int32
	ready                bool
	kvnode               *kvnode
	needWriteBackAll     bool
	shard                int
	meta                 db.DBMeta
	dbWriteBackCount     int32
	SoftLimitReachedTime int64
	unixNow              int64
}

func (s *kvstore) hasLease() bool {
	r := s.lease.hasLease()
	if !r {
		s.needWriteBackAll = true
	}
	return r
}

func (s *kvstore) isLeader() bool {
	s.raftMtx.RLock()
	defer s.raftMtx.RUnlock()
	return s.leader == s.rn.ID()
}

func (s *kvstore) getLeaderNodeID() int {
	s.raftMtx.RLock()
	defer s.raftMtx.RUnlock()
	return int(s.leader.GetNodeID())
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

	sl := s.slotsKvMap[k.slot]
	delete(sl, k.uniKey)
	if len(sl) == 0 {
		//当前slot的kv已经全部清除，如果当前slot正在迁出，结束迁出事务
		p := s.slotsTransferOut[k.slot]
		if nil != p {
			delete(s.slotsKvMap, k.slot)
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
		kv:           k,
		updateFields: map[string]*flyproto.Field{},
	}

	s.kv[groupID][unikey] = k

	s.kvcount++

	if sl := s.slotsKvMap[slot]; nil != sl {
		sl[unikey] = k
	} else {
		sl = map[string]*kv{}
		sl[unikey] = k
		s.slotsKvMap[slot] = sl
	}

	return k, nil
}

func (this *kvstore) tryKick(kv *kv) bool {
	if !kv.kickable() {
		return false
	}

	this.rn.IssueProposal(&kvProposal{
		ptype: proposal_kick,
		kv:    kv,
	})

	kv.kicking = true
	return true
}

func (s *kvstore) checkLru(ch chan struct{}) {
	defer func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}()
	if s.ready && s.leader == s.rn.ID() && atomic.LoadInt32(&s.stoped) == 0 {
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
	if keyvalue.kicking {
		return nil, errcode.New(errcode.Errcode_retry, "please retry later")
	}

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
	if !s.checkReqLimit() {
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
		})
		return
	}

	slot := sslot.Unikey2Slot(req.msg.UniKey)

	if !s.slots.Test(slot) {
		//unikey不归当前store管理,路由信息已经stale
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_route_info_stale, ""),
		})
		return
	}

	if s.slotsTransferOut[slot] != nil {
		//正在迁出
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_slot_transfering, ""),
		})
		return
	}

	if s.leader != s.rn.ID() {
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_not_leader, ""),
		})
	} else if !s.ready || s.meta == nil {
		req.from.Send(&cs.RespMessage{
			Cmd:   req.msg.Cmd,
			Seqno: req.msg.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "kvstore not start ok,please retry later"),
		})
	} else {

		var (
			err errcode.Error
			cmd cmdI
			kv  *kv
			ok  bool
		)

		groupID := sslot.StringHash(req.msg.UniKey) % len(s.kv)

		kv, ok = s.kv[groupID][req.msg.UniKey]

		if !ok {
			if req.msg.Cmd == flyproto.CmdType_Kick { //kv不在缓存中,kick操作直接返回ok
				req.from.Send(&cs.RespMessage{
					Seqno: req.msg.Seqno,
					Cmd:   req.msg.Cmd,
				})
				return
			} else {
				if s.kvcount > (s.kvnode.config.MaxCachePerStore*3)/2 {
					req.from.Send(&cs.RespMessage{
						Cmd:   req.msg.Cmd,
						Seqno: req.msg.Seqno,
						Err:   errcode.New(errcode.Errcode_retry, "kvstore busy,please retry later"),
					})
					return
				} else {
					table, key := splitUniKey(req.msg.UniKey)
					if kv, err = s.newkv(slot, groupID, req.msg.UniKey, key, table); nil != err {
						req.from.Send(&cs.RespMessage{
							Cmd:   req.msg.Cmd,
							Seqno: req.msg.Seqno,
							Err:   err,
						})
						return
					}
				}
			}
		}

		if cmd, err = s.makeCmd(kv, req); nil != err {
			req.from.Send(&cs.RespMessage{
				Cmd:   req.msg.Cmd,
				Seqno: req.msg.Seqno,
				Err:   err,
			})
		} else {
			kv.process(cmd)
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
		s.makeSnapshot(snapshotNotify)
	}
}

func (s *kvstore) processLinearizableRead(r []raft.LinearizableRead) {
	for _, v := range r {
		v.(*kvLinearizableRead).ok()
	}
}

func (s *kvstore) processConfChange(p raft.ProposalConfChange) {
	p.(*ProposalConfChange).reply(nil)
}

func (s *kvstore) stop() {
	if atomic.CompareAndSwapInt32(&s.stoped, 0, 1) {
		s.rn.Stop()
	}
}

func (s *kvstore) gotLease() {
	if s.needWriteBackAll {
		GetSugar().Info("WriteBackAll")
		s.needWriteBackAll = false
		for _, v := range s.kv {
			for _, vv := range v {
				vv.kicking = false
				err := vv.updateTask.issueFullDbWriteBack()
				if nil != err {
					break
				}
			}
		}
	}
}

func (s *kvstore) serve() {

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

	/*go func() {
		for atomic.LoadInt32(&s.stoped) == 0 {
			time.Sleep(time.Second)
			err := s.mainQueue.q.ForceAppend(1, func() {
				s.unixNow = time.Now().Unix()
				//GetSugar().Infof("store:%d kvcount:%d", s.shard, s.kvcount())
			})

			if err == queue.ErrQueueClosed {
				return
			}
		}
	}()*/

	go func() {
		defer func() {
			s.lease.stop()
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
				s.processConfChange(v.(raft.ProposalConfChange))
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode {
					if s.rn.ID() == raft.RaftInstanceID(c.NodeID) {
						GetSugar().Infof("%x RemoveFromCluster", s.rn.ID())
						s.stop()
					}
				}
			case raft.ReplayOK:
				s.ready = true
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
				becomeLeader := false
				s.raftMtx.Lock()
				s.leader = raft.RaftInstanceID(v.(raft.LeaderChange).Leader)
				if s.leader == s.rn.ID() {
					becomeLeader = true
				}
				s.raftMtx.Unlock()
				if becomeLeader {
					s.needWriteBackAll = true
					s.lease.becomeLeader()
				}
			case *SlotTransferProposal:
				if s.leader == s.rn.ID() {
					s.processSlotTransferOut(v.(*SlotTransferProposal))
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()
}

func (s *kvstore) onNotifyAddNode(from *net.UDPAddr, msg *sproto.NotifyAddNode, context int64) {
	/*if s.leader == s.rn.ID() {
		if s.rn.IsMember(uint64(msg.NodeID)) {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.NotifyAddNodeResp{
					NodeID: msg.NodeID,
					Store:  int32(s.shard),
				}))
		} else {
			//发起proposal
			s.rn.IssueConfChange(&ProposalConfChange{
				confChangeType: raftpb.ConfChangeAddNode,
				url:            fmt.Sprintf("http://%s:%d", msg.Host, msg.RaftPort),
				nodeID:         uint64(raft.MakeInstanceID(uint16(msg.NodeID), uint16(s.shard))),
				reply: func(err error) {
					s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
						&sproto.NotifyAddNodeResp{
							NodeID: msg.NodeID,
							Store:  int32(s.shard),
						}))
				},
			})
		}
	}*/
}

func (s *kvstore) onNotifyRemNode(from *net.UDPAddr, msg *sproto.NotifyRemNode, context int64) {
	/*if s.leader == s.rn.ID() {
		if !s.rn.IsMember(uint64(msg.NodeID)) {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.NotifyRemNodeResp{
					NodeID: int32(int(msg.NodeID)),
					Store:  int32(s.shard),
				}))
		} else {
			//发起proposal
			s.rn.IssueConfChange(&ProposalConfChange{
				confChangeType: raftpb.ConfChangeRemoveNode,
				nodeID:         uint64(raft.MakeInstanceID(uint16(msg.NodeID), uint16(s.shard))),
				reply: func(err error) {
					s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
						&sproto.NotifyRemNodeResp{
							NodeID: msg.NodeID,
							Store:  int32(s.shard),
						}))
				},
			})
		}
	}*/
}

func (s *kvstore) onNotifySlotTransIn(from *net.UDPAddr, msg *sproto.NotifySlotTransIn, context int64) {
	if s.leader == s.rn.ID() {
		slot := int(msg.Slot)
		if s.slots.Test(slot) {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.NotifySlotTransInResp{
					Slot: msg.Slot,
				}))
		} else {
			s.rn.IssueProposal(&SlotTransferProposal{
				slot:         slot,
				transferType: slotTransferIn,
				store:        s,
				reply: func() {
					s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
						&sproto.NotifySlotTransInResp{
							Slot: msg.Slot,
						}))
				},
			})
		}
	}
}

func (s *kvstore) processSlotTransferOut(p *SlotTransferProposal) {
	kvs := s.slotsKvMap[p.slot]
	if nil != kvs && len(kvs) > 0 {
		for _, v := range kvs {
			s.tryKick(v)
		}

		p.timer = time.AfterFunc(time.Millisecond*100, func() {
			s.mainQueue.AppendHighestPriotiryItem(p)
		})
	}
}

func (s *kvstore) onNotifySlotTransOut(from *net.UDPAddr, msg *sproto.NotifySlotTransOut, context int64) {
	if s.leader == s.rn.ID() {
		slot := int(msg.Slot)
		if !s.slots.Test(slot) {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.NotifySlotTransOutResp{
					Slot: msg.Slot,
				}))
		} else {
			p := s.slotsTransferOut[slot]

			reply := func() {
				s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
					&sproto.NotifySlotTransOutResp{
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
}

func (s *kvstore) onNotifyUpdateMeta(from *net.UDPAddr, msg *sproto.NotifyUpdateMeta, context int64) {
	if s.leader == s.rn.ID() {
		if s.meta.GetVersion() == msg.Version {
			s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
				&sproto.NotifyUpdateMetaResp{
					Store:   msg.Store,
					Version: msg.Version,
				}))
		} else {
			var meta db.DBMeta
			var err error
			var def *db.DbDef
			if def, err = db.CreateDbDefFromJsonString(msg.Meta); nil == err {
				meta, err = sql.CreateDbMeta(msg.Version, def)
			}

			if nil == err {
				s.rn.IssueProposal(&ProposalUpdateMeta{
					meta:  meta,
					store: s,
					reply: func() {
						s.kvnode.udpConn.SendTo(from, snet.MakeMessage(context,
							&sproto.NotifyUpdateMetaResp{
								Store:   msg.Store,
								Version: msg.Version,
							}))
					},
				})
			} else {
				GetSugar().Infof("onNotifyUpdateMeta ")
			}
		}
	}
}

func (s *kvstore) onUdpMsg(from *net.UDPAddr, m *snet.Message) {
	if s.ready && atomic.LoadInt32(&s.stoped) == 0 {
		switch m.Msg.(type) {
		case *sproto.NotifyAddNode:
			s.onNotifyAddNode(from, m.Msg.(*sproto.NotifyAddNode), m.Context)
		case *sproto.NotifyRemNode:
			s.onNotifyRemNode(from, m.Msg.(*sproto.NotifyRemNode), m.Context)
		case *sproto.NotifySlotTransIn:
			s.onNotifySlotTransIn(from, m.Msg.(*sproto.NotifySlotTransIn), m.Context)
		case *sproto.NotifySlotTransOut:
			s.onNotifySlotTransOut(from, m.Msg.(*sproto.NotifySlotTransOut), m.Context)
		case *sproto.NotifyUpdateMeta:
			s.onNotifyUpdateMeta(from, m.Msg.(*sproto.NotifyUpdateMeta), m.Context)
		}
	}
}
