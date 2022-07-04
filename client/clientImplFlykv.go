package client

import (
	"container/list"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sort"
	"sync"
	"time"
)

var QueryRouteInfoDuration time.Duration = time.Millisecond * 1000

func QueryRouteInfo(pdAddr []*net.UDPAddr, req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	if resp, err := snet.UdpCall(pdAddr, req, &sproto.QueryRouteInfoResp{}, time.Second); nil == err {
		return resp.(*sproto.QueryRouteInfoResp)
	} else {
		return nil
	}
}

type kvnode struct {
	waitSend *list.List
	id       int
	service  string
	session  *flynet.Socket
	setID    int
}

type store struct {
	waitSend      *list.List
	leaderVersion int64
	id            int
	leader        *kvnode
	slots         *bitmap.Bitmap
	setID         int
}

type set struct {
	setID  int
	nodes  map[int]*kvnode
	stores map[int]*store
}

//向pd获取路由信息，请求直接发往flykv
type clientImplFlykv struct {
	mu             sync.Mutex
	closed         bool
	waitResp       map[int64]*cmdContext
	waitSend       *list.List  //len(avaliableConns)==0时被排队等待发送的请求
	notifyQueue    EventQueueI //响应回调的事件队列
	notifyPriority int         //回调事件优先级
	version        int64
	sets           map[int]*set
	slotToStore    map[int]*store
}

func (this *clientImplFlykv) checkKvnode(n *kvnode) bool {
	if s, ok := this.sets[n.setID]; ok {
		if kvnode, ok := s.nodes[n.id]; ok {
			return kvnode == n
		}
	}
	return false
}

func (this *clientImplFlykv) checkStore(st *store) bool {
	if s, ok := this.sets[st.setID]; ok {
		if store, ok := s.stores[st.id]; ok {
			return store == st
		}
	}
	return false
}

func (this *clientImplFlykv) sendAgain(cmd *cmdContext) {
	this.mu.Lock()
	if nil != this.waitResp[cmd.req.Seqno] {
		if this.closed {
			delete(this.waitResp, cmd.req.Seqno)
			cmd.stopTimer()
			this.mu.Unlock()
			cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_error, "client closed")), nil)
		} else {
			if store, ok := this.slotToStore[cmd.slot]; ok {
				this.storeSend(store, cmd)
			} else {
				//找不到对应store,先存起来，等路由信息更新后再尝试
				cmd.l = this.waitSend
				cmd.listElement = this.waitSend.PushBack(cmd)
			}
			this.mu.Unlock()
		}
	} else {
		this.mu.Unlock()
	}
}

func (this *clientImplFlykv) onErrNotLeader(store *store, cmd *cmdContext) {
	if nil != store.leader && store.leaderVersion != cmd.leaderVersion {
		//leader已经变更，向新的leader发送
		cmd.leaderVersion = store.leaderVersion
		this.kvnodeSend(store.leader, cmd)
	} else if nil != store.leader && store.leaderVersion == cmd.leaderVersion {
		store.leader = nil
	}

	if nil == store.leader {
		cmd.l = store.waitSend
		cmd.listElement = store.waitSend.PushBack(cmd)
		if store.waitSend.Len() == 1 {
			this.queryLeader(store)
		}
	}
}

func (this *clientImplFlykv) onResponse(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {
		this.mu.Lock()
		ctx := this.waitResp[msg.Seqno]
		if nil == ctx {
			this.mu.Unlock()
			return
		}

		if errcode.GetCode(msg.Err) == errcode.Errcode_not_leader {
			if store, ok := this.slotToStore[ctx.slot]; ok {
				this.onErrNotLeader(store, ctx)
			} else {
				ctx.l = this.waitSend
				ctx.listElement = this.waitSend.PushBack(ctx)
			}
			this.mu.Unlock()
		} else {
			switch errcode.GetCode(msg.Err) {
			case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering, errcode.Errcode_retry:
				this.mu.Unlock()
				time.AfterFunc(resendDelay, func() {
					this.sendAgain(ctx)
				})
			default:
				delete(this.waitResp, msg.Seqno)
				ctx.stopTimer()
				this.mu.Unlock()
				var ret interface{}
				switch cmd {
				case protocol.CmdType_Get:
					ret = onGetResp(ctx, msg.Err, msg.Data.(*protocol.GetResp))
				case protocol.CmdType_Set:
					ret = onSetResp(ctx, msg.Err, msg.Data.(*protocol.SetResp))
				case protocol.CmdType_SetNx:
					ret = onSetNxResp(ctx, msg.Err, msg.Data.(*protocol.SetNxResp))
				case protocol.CmdType_CompareAndSet:
					ret = onCompareAndSetResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetResp))
				case protocol.CmdType_CompareAndSetNx:
					ret = onCompareAndSetNxResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetNxResp))
				case protocol.CmdType_Del:
					ret = onDelResp(ctx, msg.Err, msg.Data.(*protocol.DelResp))
				case protocol.CmdType_IncrBy:
					ret = onIncrByResp(ctx, msg.Err, msg.Data.(*protocol.IncrByResp))
				case protocol.CmdType_Kick:
					ret = onKickResp(ctx, msg.Err, msg.Data.(*protocol.KickResp))
				default:
					ret = ctx.getErrorResult(errcode.New(errcode.Errcode_error, "invaild response"))
				}
				ctx.doCallBack(this.notifyQueue, this.notifyPriority, ret, nil)
			}
		}
	}
}

func (this *clientImplFlykv) connectKvnode(kvnode *kvnode) {
	go func() {
		c := cs.NewConnector("tcp", kvnode.service, flynet.OutputBufLimit{
			OutPutLimitSoft:        1024 * 1024 * 10,
			OutPutLimitSoftSeconds: 10,
			OutPutLimitHard:        1024 * 1024 * 50,
		})
		session, err := c.Dial(time.Second * 5)
		this.mu.Lock()
		defer this.mu.Unlock()
		if this.closed || !this.checkKvnode(kvnode) {
			return
		} else {
			if nil == err {
				kvnode.session = session
				session.SetRecvTimeout(recvTimeout)
				session.SetInBoundProcessor(cs.NewRespInboundProcessor())
				session.SetEncoder(&cs.ReqEncoder{})
				session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
					GetSugar().Infof("socket close %v", reason)
					this.mu.Lock()
					kvnode.session = nil
					ctxs := []*cmdContext{}
					for _, v := range this.waitResp {
						if v.session == session {
							delete(this.waitResp, v.req.Seqno)
							v.stopTimer()
							ctxs = append(ctxs, v)
						}
					}
					this.mu.Unlock()
					err := errcode.New(errcode.Errcode_error, "lose connection")
					for _, v := range ctxs {
						v.doCallBack(this.notifyQueue, this.notifyPriority, v.getErrorResult(err), nil)
					}
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					this.onResponse(msg.(*cs.RespMessage))
				})

				for v := kvnode.waitSend.Front(); nil != v; v = kvnode.waitSend.Front() {
					cmd := kvnode.waitSend.Remove(v).(*cmdContext)
					this.kvnodeSend(kvnode, cmd)
				}
			} else if kvnode.waitSend.Len() > 0 {
				time.AfterFunc(time.Second, func() { this.connectKvnode(kvnode) })
			}
		}
	}()
}

func (this *clientImplFlykv) kvnodeSend(kvnode *kvnode, cmd *cmdContext) {
	if nil != kvnode.session {
		cmd.l = nil
		cmd.listElement = nil
		if cmd.req.Timeout = uint32(cmd.deadline.Sub(time.Now()) / time.Millisecond); cmd.req.Timeout > 0 {
			cmd.session = kvnode.session
			kvnode.session.Send(cmd.req)
		}
	} else {
		cmd.l = kvnode.waitSend
		cmd.listElement = kvnode.waitSend.PushBack(cmd)
		if kvnode.waitSend.Len() == 1 {
			this.connectKvnode(kvnode)
		}
	}
}

func (this *clientImplFlykv) queryLeader(store *store) {
	if !this.closed && this.checkStore(store) {
		set := this.sets[store.setID]
		nodes := []string{}
		for _, v := range set.nodes {
			nodes = append(nodes, v.service)
		}

		if len(nodes) > 0 {
			go func() {
				var leader int
				if r, err := snet.UdpCall(nodes, &sproto.QueryLeader{Store: int32(store.id)}, &sproto.QueryLeaderResp{}, time.Second); nil == err {
					leader = int(r.(*sproto.QueryLeaderResp).Leader)
				}
				this.mu.Lock()
				if this.checkStore(store) {
					set := this.sets[store.setID]
					if leaderNode := set.nodes[leader]; nil != leaderNode {
						store.leaderVersion++
						store.leader = leaderNode
						for v := store.waitSend.Front(); nil != v; v = store.waitSend.Front() {
							cmd := store.waitSend.Remove(v).(*cmdContext)
							cmd.leaderVersion = store.leaderVersion
							this.kvnodeSend(leaderNode, cmd)
						}
					} else {
						time.AfterFunc(time.Millisecond*100, func() {
							this.mu.Lock()
							defer this.mu.Unlock()
							this.queryLeader(store)
						})
					}

				}
				this.mu.Unlock()
			}()
		} else {
			time.AfterFunc(time.Millisecond*100, func() {
				this.mu.Lock()
				defer this.mu.Unlock()
				this.queryLeader(store)
			})
		}
	}
}

func (this *clientImplFlykv) storeSend(store *store, cmd *cmdContext) {
	cmd.store = uint64(store.setID)<<32 + uint64(store.id)
	cmd.req.Store = store.id
	if nil == store.leader {
		cmd.l = store.waitSend
		cmd.listElement = store.waitSend.PushBack(cmd)
		if store.waitSend.Len() == 1 {
			this.queryLeader(store)
		}
	} else {
		cmd.leaderVersion = store.leaderVersion
		this.kvnodeSend(store.leader, cmd)
	}
}

func (this *clientImplFlykv) onTimeout(cmd *cmdContext) {
	cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_timeout, "timeout")), func() {
		this.mu.Lock()
		cmd.stopTimer()
		delete(this.waitResp, cmd.req.Seqno)
		if nil != cmd.listElement {
			cmd.l.Remove(cmd.listElement)
		}
		this.mu.Unlock()
	})
}

func (this *clientImplFlykv) exec(cmd *cmdContext) {
	var errCode errcode.Error
	this.mu.Lock()
	if this.closed {
		errCode = errcode.New(errcode.Errcode_error, "client closed")
	} else if len(this.waitResp) > maxPendingSize {
		errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
	} else {
		this.waitResp[cmd.req.Seqno] = cmd
		cmd.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
		cmd.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, func() { this.onTimeout(cmd) })
		cmd.slot = slot.Unikey2Slot(cmd.table + ":" + cmd.key)
		if store, ok := this.slotToStore[cmd.slot]; ok {
			this.storeSend(store, cmd)
		} else {
			//找不到对应store,先存起来，等路由信息更新后再尝试
			cmd.l = this.waitSend
			cmd.listElement = this.waitSend.PushBack(cmd)
		}
	}
	this.mu.Unlock()

	if nil != errCode {
		cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errCode), nil)
	}
}

func (this *clientImplFlykv) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) {
	if nil == resp {
		return
	}

	if this.version == resp.Version {
		return
	}

	this.version = resp.Version

	for _, v := range resp.Sets {
		s, ok := this.sets[int(v.SetID)]
		if ok {
			for k, _ := range v.Stores {
				ss := s.stores[int(v.Stores[k])]
				ss.slots, _ = bitmap.CreateFromJson(v.Slots[k])
			}

			localKvnodes := []int32{}
			for _, vv := range s.nodes {
				localKvnodes = append(localKvnodes, int32(vv.id))
			}

			respKvnodes := [][]int32{}
			for k, vv := range v.Kvnodes {
				respKvnodes = append(respKvnodes, []int32{vv.NodeID, int32(k)})
			}

			sort.Slice(localKvnodes, func(i, j int) bool {
				return localKvnodes[i] < localKvnodes[j]
			})

			sort.Slice(respKvnodes, func(i, j int) bool {
				return respKvnodes[i][0] < respKvnodes[j][0]
			})

			add := [][]int32{}
			remove := []int32{}

			i := 0
			j := 0

			for i < len(respKvnodes) && j < len(localKvnodes) {
				if respKvnodes[i][0] == localKvnodes[j] {
					i++
					j++
				} else if respKvnodes[i][0] > localKvnodes[j] {
					remove = append(remove, localKvnodes[j])
					j++
				} else {
					add = append(add, respKvnodes[i])
					i++
				}
			}

			if len(respKvnodes[i:]) > 0 {
				add = append(add, respKvnodes[i:]...)
			}

			if len(localKvnodes[j:]) > 0 {
				remove = append(remove, localKvnodes[j:]...)
			}

			for _, vv := range add {
				n := &kvnode{
					id:       int(vv[0]),
					service:  fmt.Sprintf("%s:%d", v.Kvnodes[vv[1]].Host, v.Kvnodes[vv[1]].ServicePort),
					waitSend: list.New(),
					setID:    int(v.SetID),
				}
				s.nodes[n.id] = n
			}

			for _, vv := range remove {
				kvnode := s.nodes[int(vv)]
				delete(s.nodes, int(vv))
				if nil != kvnode.session {
					kvnode.session.Close(nil, 0)
					kvnode.session = nil
				} else {
					for v := kvnode.waitSend.Front(); nil != v; v = kvnode.waitSend.Front() {
						cmd := kvnode.waitSend.Remove(v).(*cmdContext)
						cmd.l = this.waitSend
						cmd.listElement = this.waitSend.PushBack(cmd)
					}
				}

				for _, store := range s.stores {
					if store.leader == kvnode {
						store.leader = nil
					}
				}
			}

		} else {
			s = &set{
				setID:  int(v.SetID),
				nodes:  map[int]*kvnode{},
				stores: map[int]*store{},
			}

			for _, vv := range v.Kvnodes {
				n := &kvnode{
					id:       int(vv.NodeID),
					service:  fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort),
					waitSend: list.New(),
					setID:    int(v.SetID),
				}
				s.nodes[n.id] = n
			}

			for k, vv := range v.Stores {
				st := &store{
					id:       int(vv),
					waitSend: list.New(),
					setID:    int(v.SetID),
				}
				st.slots, _ = bitmap.CreateFromJson(v.Slots[k])
				s.stores[st.id] = st
			}
			this.sets[s.setID] = s
		}
	}

	for _, v := range resp.RemoveSets {
		if s, ok := this.sets[int(v)]; ok {

			for _, kvnode := range s.nodes {
				if nil != kvnode.session {
					kvnode.session.Close(nil, 0)
					kvnode.session = nil
				} else {
					for v := kvnode.waitSend.Front(); nil != v; v = kvnode.waitSend.Front() {
						cmd := kvnode.waitSend.Remove(v).(*cmdContext)
						cmd.l = this.waitSend
						cmd.listElement = this.waitSend.PushBack(cmd)
					}
				}
			}

			for _, store := range s.stores {
				//将待转发请求回收
				for v := store.waitSend.Front(); nil != v; v = store.waitSend.Front() {
					cmd := store.waitSend.Remove(v).(*cmdContext)
					cmd.l = this.waitSend
					cmd.listElement = this.waitSend.PushBack(cmd)
				}
			}
			delete(this.sets, int(v))
		}
	}

	this.slotToStore = map[int]*store{}
	for _, set := range this.sets {
		for _, store := range set.stores {
			slots := store.slots.GetOpenBits()
			for _, v := range slots {
				this.slotToStore[v] = store
			}
		}
	}

	//GetSugar().Infof("onQueryRouteInfoResp %v", this.slotToStore)
	//路由信息更新,尝试发送waitSend中的Cmd
	for ele := this.waitSend.Front(); nil != ele; {
		next := ele.Next()
		cmd := ele.Value.(*cmdContext)
		if store, ok := this.slotToStore[cmd.slot]; ok {
			this.waitSend.Remove(ele)
			this.storeSend(store, cmd)
		}
		ele = next
	}
}

func (this *clientImplFlykv) queryRouteInfo(pdAddr []*net.UDPAddr) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return
	} else {
		req := &sproto.QueryRouteInfo{
			Version: this.version,
		}

		for kk, _ := range this.sets {
			req.Sets = append(req.Sets, int32(kk))
		}

		go func() {
			resp := QueryRouteInfo(pdAddr, req)
			this.mu.Lock()
			defer this.mu.Unlock()
			if this.closed {
				return
			} else {
				this.onQueryRouteInfoResp(resp)
				delay := QueryRouteInfoDuration
				if this.waitSend.Len() > 0 {
					delay = time.Millisecond * 50
				}
				time.AfterFunc(delay, func() {
					this.queryRouteInfo(pdAddr)
				})
			}
		}()
	}
}

func (this *clientImplFlykv) start(pdAddr []*net.UDPAddr) {
	this.queryRouteInfo(pdAddr)
}

func (this *clientImplFlykv) close() {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return
	} else {
		this.closed = true
		for _, set := range this.sets {
			for _, kvnode := range set.nodes {
				if nil != kvnode.session {
					kvnode.session.Close(nil, 0)
					kvnode.session = nil
				}
			}
		}
	}
}
