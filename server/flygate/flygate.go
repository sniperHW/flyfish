package flygate

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/movingAverage"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sort"
	"strings"
	"sync"
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

func (r *routeInfo) onQueryRouteInfoResp(gate *gate, resp *sproto.QueryRouteInfoResp) (change bool) {

	GetSugar().Debugf("onQueryRouteInfoResp version:%d", resp.Version)
	change = r.version != resp.Version

	if change {

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
						//set:          s,
						config: r.config,
						gate:   gate,
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
						//set:          s,
						config: r.config,
						gate:   gate,
					}
					s.nodes[n.id] = n
				}

				for k, vv := range v.Stores {
					st := &store{
						id:           int(vv),
						waittingSend: list.New(),
						set:          s,
						config:       r.config,
						gate:         gate,
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
					vv.removed = true
					if nil != vv.session {
						vv.session.Close(nil, 0)
					}
				}
				for _, vv := range s.stores {
					vv.removed = true
				}
			}
		}

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

type gate struct {
	config          *Config
	closed          int32
	closeCh         chan struct{}
	listener        *cs.Listener
	totalPendingMsg int64
	pendingMsg      *list.List //尚无正确路由信息的请求
	muC             sync.Mutex
	clients         map[*flynet.Socket]*flynet.Socket
	routeInfo       routeInfo
	queryTimer      *time.Timer
	mainQueue       *queue.PriorityQueue
	serviceAddr     string
	seqCounter      int64
	pdAddr          []*net.UDPAddr
	msgPerSecond    *movingAverage.MovingAverage //每秒客户端转发请求量的移动平均值
	msgRecv         int32
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

func NewFlyGate(config *Config, service string) (*gate, error) {
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
		pendingMsg:   list.New(),
		serviceAddr:  service,
		msgPerSecond: movingAverage.New(5),
		closeCh:      make(chan struct{}),
	}

	pdService := strings.Split(config.PdService, ";")

	for _, v := range pdService {
		if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
			g.pdAddr = append(g.pdAddr, addr)
		}
	}

	if len(g.pdAddr) == 0 {
		return nil, errors.New("pd is empty")
	}

	err := g.start()

	g.refreshMsgPerSecond()

	return g, err
}

func (g *gate) callInQueue(priority int, fn func()) {
	g.mainQueue.ForceAppend(priority, fn)
}

func (g *gate) afterFunc(delay time.Duration, fn func()) *time.Timer {
	return time.AfterFunc(delay, func() {
		g.callInQueue(1, fn)
	})
}

func (g *gate) refreshMsgPerSecond() {
	if atomic.LoadInt32(&g.closed) == 0 {
		msgRecv := atomic.LoadInt32(&g.msgRecv)
		atomic.AddInt32(&g.msgRecv, -msgRecv)
		g.msgPerSecond.Add(int(msgRecv))
		time.AfterFunc(time.Second, g.refreshMsgPerSecond)
	}
}

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		g.muC.Lock()
		g.clients[session] = session
		g.muC.Unlock()
		session.SetEncoder(&encoder{})
		session.SetInBoundProcessor(NewCliReqInboundProcessor())
		session.SetRecvTimeout(flyproto.PingTime * 10)

		session.SetCloseCallBack(func(session *flynet.Socket, reason error) {
			g.muC.Lock()
			delete(g.clients, session)
			g.muC.Unlock()
		})

		session.BeginRecv(func(session *flynet.Socket, v interface{}) {
			if atomic.LoadInt32(&g.closed) == 1 {
				//服务关闭不再接受新新的请求
				return
			}
			atomic.AddInt32(&g.msgRecv, 1)
			msg := v.(*forwordMsg)
			msg.cli = session
			g.mainQueue.ForceAppend(0, msg)
		})
	}, g.onScanner)
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
			if msg.slot >= 0 && msg.slot < slot.SlotCount {
				msg.deadlineTimer = g.afterFunc(msg.deadline.Sub(time.Now()), msg.dropReply)
				g.seqCounter++
				msg.seqno = g.seqCounter
				msg.totalPendingMsg = &g.totalPendingMsg
				if atomic.AddInt64(msg.totalPendingMsg, 1) > int64(g.config.MaxPendingMsg) {
					msg.replyErr(errcode.New(errcode.Errcode_retry, "gate busy,please retry later"))
				} else {
					s, ok := g.routeInfo.slotToStore[msg.slot]
					if !ok {
						GetSugar().Infof("slot%d has no route info", msg.slot)
						msg.add(nil, g.pendingMsg)
					} else {
						if !s.onCliMsg(msg) {
							msg.replyErr(errcode.New(errcode.Errcode_retry, "gate busy,please retry later"))
						}
					}
				}
			}
		case func():
			v.(func())()
		}
	}
}

func (g *gate) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) {
	if nil != resp && g.routeInfo.onQueryRouteInfoResp(g, resp) {
		GetSugar().Infof("Update QueryRouteInfo")
		//路由更新，尝试处理没有正确路由信息的请求
		size := g.pendingMsg.Len()
		v := g.pendingMsg.Front()
		now := time.Now()
		for i := 0; i < size; i++ {
			msg := v.Value.(*forwordMsg)
			v = v.Next()
			msg.removeList()
			if msg.deadline.After(now) {
				s, ok := g.routeInfo.slotToStore[msg.slot]
				if !ok {
					msg.add(nil, g.pendingMsg)
				} else {
					if !s.onCliMsg(msg) {
						msg.replyErr(errcode.New(errcode.Errcode_retry, "gate busy,please retry later"))
					}
				}
			} else {
				msg.dropReply()
			}
		}
	}
}

var QueryRouteInfoDuration time.Duration = time.Millisecond * 200

func (g *gate) queryRouteInfo() {
	req := &sproto.QueryRouteInfo{
		Version: g.routeInfo.version,
	}

	for kk, _ := range g.routeInfo.sets {
		req.Sets = append(req.Sets, int32(kk))
	}

	go func() {
		resp := doQueryRouteInfo(g.pdAddr, req)
		g.callInQueue(1, func() {
			g.onQueryRouteInfoResp(resp)
			delay := QueryRouteInfoDuration
			if g.pendingMsg.Len() > 0 {
				delay = time.Millisecond * 50
			}
			g.startQueryTimer(delay)
		})
	}()
}

func (g *gate) startQueryTimer(delay time.Duration) {
	g.queryTimer = time.AfterFunc(delay, func() {
		g.queryRouteInfo()
	})
}

func (g *gate) start() error {
	var err error

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

	go func() {
		heartbeatConn, _ := flynet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			msg := &sproto.FlyGateHeartBeat{
				GateService:  g.serviceAddr,
				MsgPerSecond: int32(g.msgPerSecond.GetAverage()),
			}

			for _, v := range g.pdAddr {
				if err := heartbeatConn.SendTo(v, snet.MakeMessage(0, msg)); nil != err {
					GetSugar().Errorf("send heartbeat to %v err:%v", v, err)
				}
			}

			select {
			case <-g.closeCh:
				return
			case <-ticker.C:
			}
		}
	}()

	go g.mainLoop()

	g.startListener()

	g.startQueryTimer(QueryRouteInfoDuration)

	return nil
}

func (g *gate) checkCondition(notiyCh chan struct{}, fn func() bool) {
	g.callInQueue(1, func() {
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
	if atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		g.listener.Close()

		close(g.closeCh)

		//等待所有消息处理完
		g.waitCondition(func() bool {
			return g.totalPendingMsg == 0
		})

		g.muC.Lock()
		for _, v := range g.clients {
			go v.Close(nil, time.Second*5)
		}
		g.muC.Unlock()

		g.waitCondition(func() bool {
			g.muC.Lock()
			defer g.muC.Unlock()
			return len(g.clients) == 0
		})

		g.mainQueue.Close()

	}
}
