package flygate

import (
	"container/list"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/movingAverage"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type encoder struct {
}

func (this *encoder) EnCode(o interface{}, buff *buffer.Buffer) error {
	b, ok := o.([]byte)
	if !ok {
		return errors.New("invaild o")
	}

	buff.AppendBytes(b)

	return nil
}

type set struct {
	setID     int
	nodes     map[int]*kvnode
	stores    map[int]*store
	routeInfo *routeInfo
	removed   bool
}

type routeInfo struct {
	version     int64
	sets        map[int]*set
	slotToStore map[int]*store
	config      *Config
	mainQueue   *queue.PriorityQueue
}

func (r *routeInfo) onQueryRouteInfoResp(gate *gate, resp *sproto.QueryRouteInfoResp) (oldSlotToStore map[int]*store) {

	//GetSugar().Infof("onQueryRouteInfoResp %v", resp)

	change := r.version != resp.Version
	r.version = resp.Version

	for _, v := range resp.Sets {
		s, ok := r.sets[int(v.SetID)]
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
					id:           int(vv[0]),
					service:      fmt.Sprintf("%s:%d", v.Kvnodes[vv[1]].Host, v.Kvnodes[vv[1]].ServicePort),
					waittingSend: list.New(),
					waitResponse: map[int64]*forwordMsg{},
					set:          s,
					config:       r.config,
					mainQueue:    r.mainQueue,
					gate:         gate,
				}
				s.nodes[n.id] = n
			}

			for _, vv := range remove {
				n := s.nodes[int(vv)]
				delete(s.nodes, int(vv))
				n.removed = true
				if nil != n.session {
					n.session.Close(nil, 0)
				}
			}

		} else {
			s := &set{
				setID:     int(v.SetID),
				nodes:     map[int]*kvnode{},
				stores:    map[int]*store{},
				routeInfo: r,
			}

			for _, vv := range v.Kvnodes {
				n := &kvnode{
					id:           int(vv.NodeID),
					service:      fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort),
					waittingSend: list.New(),
					waitResponse: map[int64]*forwordMsg{},
					set:          s,
					config:       r.config,
					mainQueue:    r.mainQueue,
					gate:         gate,
				}
				s.nodes[n.id] = n
			}

			for k, vv := range v.Stores {
				st := &store{
					id:           int(vv),
					waittingSend: list.New(),
					set:          s,
					config:       r.config,
					mainQueue:    r.mainQueue,
				}
				st.slots, _ = bitmap.CreateFromJson(v.Slots[k])
				s.stores[st.id] = st
			}

			r.sets[s.setID] = s
		}
	}

	for _, v := range resp.RemoveSets {
		if s, ok := r.sets[int(v)]; ok {
			s.removed = true
			delete(r.sets, int(v))
			for _, vv := range s.nodes {
				if nil != vv.session {
					vv.session.Close(nil, 0)
				}
			}
		}
	}

	if change {
		oldSlotToStore = r.slotToStore
		r.slotToStore = map[int]*store{}
		for _, v := range r.sets {
			for _, vv := range v.stores {
				slots := vv.slots.GetOpenBits()
				for _, vvv := range slots {
					r.slotToStore[vvv] = vv
				}
			}
		}
	}

	return
}

type routeErrorAndSlotTransferingReqMgr struct {
	routeErrorReqList  *list.List //路由信息stale导致错误转发的请求，待路由信息更新后重发
	slotTransferingReq map[int]*list.List
}

func (m *routeErrorAndSlotTransferingReqMgr) empty() bool {
	if m.routeErrorReqList.Len() > 0 {
		return false
	} else if len(m.slotTransferingReq) > 0 {
		return false
	} else {
		return true
	}
}

func (m *routeErrorAndSlotTransferingReqMgr) addRouteErrorReq(msg *forwordMsg) {
	msg.l = m.routeErrorReqList
	msg.listElement = m.routeErrorReqList.PushBack(msg)
}

func (m *routeErrorAndSlotTransferingReqMgr) addSlotTransferingReq(msg *forwordMsg) {
	l := m.slotTransferingReq[msg.slot]
	if nil == l {
		l = list.New()
		m.slotTransferingReq[msg.slot] = l
	}

	msg.l = l
	msg.listElement = l.PushBack(msg)
}

type gate struct {
	config          *Config
	stopOnce        int32
	startOnce       int32
	listener        *cs.Listener
	totalPendingMsg int64
	clients         map[*flynet.Socket]*flynet.Socket
	routeInfo       routeInfo
	queryTimer      *time.Timer
	mainQueue       *queue.PriorityQueue
	serviceAddr     string
	seqCounter      int64
	pdToken         string
	pdAddr          []*net.UDPAddr
	reSendReqMgr    routeErrorAndSlotTransferingReqMgr
	msgPerSecond    *movingAverage.MovingAverage //每秒客户端转发请求量的移动平均值
	heartBeatUdp    *flynet.Udp
}

func doQueryRouteInfo(pdAddr []*net.UDPAddr, req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	if len(pdAddr) == 0 {
		GetSugar().Fatalf("PdService is empty")
		return nil
	}

	context := snet.MakeUniqueContext()
	if resp := snet.UdpCall(pdAddr, snet.MakeMessage(context, req), time.Second, func(respCh chan interface{}, r interface{}) {
		if m, ok := r.(*snet.Message); ok {
			if resp, ok := m.Msg.(*sproto.QueryRouteInfoResp); ok && context == m.Context {
				select {
				case respCh <- resp:
				default:
				}
			}
		}
	}); nil != resp {
		return resp.(*sproto.QueryRouteInfoResp)
	} else {
		return nil
	}
}

func NewFlyGate(config *Config, service string) *gate {
	mainQueue := queue.NewPriorityQueue(2)
	g := &gate{
		config:    config,
		clients:   map[*flynet.Socket]*flynet.Socket{},
		mainQueue: mainQueue,
		routeInfo: routeInfo{
			config:      config,
			sets:        map[int]*set{},
			slotToStore: map[int]*store{},
			mainQueue:   mainQueue,
		},
		reSendReqMgr: routeErrorAndSlotTransferingReqMgr{
			routeErrorReqList:  list.New(),
			slotTransferingReq: map[int]*list.List{},
		},
		serviceAddr:  service,
		msgPerSecond: movingAverage.New(5),
	}

	pdService := strings.Split(config.PdService, ";")

	for _, v := range pdService {
		if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
			g.pdAddr = append(g.pdAddr, addr)
		}
	}

	g.heartBeatUdp, _ = flynet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
	tmp := md5.Sum([]byte(g.serviceAddr + "magicNum"))
	g.pdToken = base64.StdEncoding.EncodeToString(tmp[:])

	return g
}

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		g.mainQueue.ForceAppend(1, func() {
			g.clients[session] = session
			session.SetEncoder(&encoder{})
			session.SetInBoundProcessor(NewCliReqInboundProcessor())
			session.SetRecvTimeout(flyproto.PingTime * 10)

			session.SetCloseCallBack(func(session *flynet.Socket, reason error) {
				g.mainQueue.ForceAppend(1, func() {
					delete(g.clients, session)
				})
			})

			session.BeginRecv(func(session *flynet.Socket, v interface{}) {
				if atomic.LoadInt32(&g.stopOnce) == 1 {
					//服务关闭不再接受新新的请求
					return
				}
				g.msgPerSecond.Add(1)
				msg := v.(*forwordMsg)
				msg.cli = session
				g.mainQueue.ForceAppend(0, msg)
			})
		})
	})
	GetSugar().Infof("flygate start on %s", g.serviceAddr)
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (g *gate) mainLoop() {
	for {
		_, v := g.mainQueue.Pop()
		switch v.(type) {
		case *forwordMsg:
			msg := v.(*forwordMsg)
			s, ok := g.routeInfo.slotToStore[msg.slot]
			if !ok {
				replyCliError(msg.cli, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
			} else {
				g.seqCounter++
				msg.seqno = g.seqCounter
				msg.totalPendingMsg = &g.totalPendingMsg
				msg.store = s
				s.onCliMsg(msg)
			}
		case func():
			v.(func())()
		}
	}
}

func (g *gate) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) {
	oldSlotToStore := g.routeInfo.onQueryRouteInfoResp(g, resp)

	for v := g.reSendReqMgr.routeErrorReqList.Front(); nil != v; v = g.reSendReqMgr.routeErrorReqList.Front() {
		req := g.reSendReqMgr.routeErrorReqList.Remove(v).(*forwordMsg)
		req.l = nil
		req.listElement = nil
		s, ok := g.routeInfo.slotToStore[req.slot]
		if !ok {
			req.deadlineTimer.Stop()
			replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
		} else {
			req.store = s
			s.onCliMsg(req)
		}
	}

	if nil != oldSlotToStore && len(oldSlotToStore) > 0 {
		for k, v := range g.reSendReqMgr.slotTransferingReq {
			oldStore := oldSlotToStore[k]
			newStore := g.routeInfo.slotToStore[k]
			if oldStore.id != newStore.id {
				//store发生了变更，说明slotTransfering已经完成，重发请求
				for vv := v.Front(); nil != vv; vv = v.Front() {
					req := v.Remove(vv).(*forwordMsg)
					req.l = nil
					req.listElement = nil
					s, ok := g.routeInfo.slotToStore[req.slot]
					if !ok {
						req.deadlineTimer.Stop()
						replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
					} else {
						req.store = s
						s.onCliMsg(req)
					}
				}
				delete(g.reSendReqMgr.slotTransferingReq, k)
			}
		}
	}
}

func (g *gate) onForwordError(errCode int16, msg *forwordMsg) {
	if !time.Now().After(msg.deadline) {
		empty := g.reSendReqMgr.empty()
		if errCode == errcode.Errcode_route_info_stale {
			g.reSendReqMgr.addRouteErrorReq(msg)
		} else {
			g.reSendReqMgr.addSlotTransferingReq(msg)
		}
		if empty && g.queryTimer.Stop() {
			g.startQueryTimer(time.Nanosecond)
		}
	} else {
		msg.dropReply()
	}
}

func (g *gate) queryRouteInfo() {
	req := &sproto.QueryRouteInfo{
		Version: g.routeInfo.version,
	}

	for kk, _ := range g.routeInfo.sets {
		req.Sets = append(req.Sets, int32(kk))
	}

	go func() {
		resp := doQueryRouteInfo(g.pdAddr, req)
		g.mainQueue.ForceAppend(1, func() {
			nextTimeout := time.Millisecond * 100
			if nil != resp {
				g.onQueryRouteInfoResp(resp)
				if !g.reSendReqMgr.empty() {
					nextTimeout = time.Second * 10
				}
			}
			g.startQueryTimer(nextTimeout)
		})
	}()
}

func (g *gate) startQueryTimer(timeout time.Duration) {
	g.queryTimer = time.AfterFunc(timeout, func() {
		g.queryRouteInfo()
	})
}

func (g *gate) Start() error {
	if len(g.pdAddr) == 0 {
		return errors.New("pd is empty")
	}

	var err error
	if atomic.CompareAndSwapInt32(&g.startOnce, 0, 1) {
		g.listener, err = cs.NewListener("tcp", g.serviceAddr, flynet.OutputBufLimit{
			OutPutLimitSoft:        1024 * 1024 * 10,
			OutPutLimitSoftSeconds: 10,
			OutPutLimitHard:        1024 * 1024 * 50,
		}, verifyLogin)

		if nil != err {
			return err
		}

		for {

			resp := doQueryRouteInfo(g.pdAddr, &sproto.QueryRouteInfo{})

			if nil != resp {
				g.routeInfo.onQueryRouteInfoResp(g, resp)
				break
			}
		}

		var heartbeat func()
		heartbeat = func() {
			msg := &sproto.FlyGateHeartBeat{
				GateService:  g.serviceAddr,
				Token:        g.pdToken,
				MsgPerSecond: int32(g.msgPerSecond.GetAverage()),
			}

			for _, v := range g.pdAddr {
				if nil != g.heartBeatUdp.SendTo(v, snet.MakeMessage(0, msg)) {
					return
				}
			}

			time.AfterFunc(time.Second, heartbeat)
		}

		heartbeat()

		go g.mainLoop()

		g.startListener()

		g.startQueryTimer(time.Second * 10)
	}

	return nil
}

func (g *gate) checkCondition(notiyCh chan struct{}, fn func() bool) {
	g.mainQueue.ForceAppend(1, func() {
		if fn() {
			select {
			case notiyCh <- struct{}{}:
			default:
			}
		} else {
			go func() {
				time.Sleep(time.Millisecond * 100)
				g.checkCondition(notiyCh, fn)
			}()
		}
	})
}

func (g *gate) waitCondition(fn func() bool) {
	waitCh := make(chan struct{})
	g.checkCondition(waitCh, fn)
	<-waitCh
}

func (g *gate) Stop() {
	if atomic.CompareAndSwapInt32(&g.stopOnce, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		g.listener.Close()

		g.heartBeatUdp.Close()

		//等待所有消息处理完
		g.waitCondition(func() bool {
			return g.totalPendingMsg == 0
		})

		for _, v := range g.clients {
			v.Close(nil, time.Second*5)
		}

		g.waitCondition(func() bool {
			return len(g.clients) == 0
		})

		g.mainQueue.Close()

	}
}
