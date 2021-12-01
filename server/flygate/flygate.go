package flygate

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	//"os"
	"crypto/md5"
	"encoding/base64"
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

func (r *routeInfo) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) {

	GetSugar().Infof("onQueryRouteInfoResp %v", resp)

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
					pendingReq:   map[int64]*relayMsg{},
					set:          s,
					config:       r.config,
					mainQueue:    r.mainQueue,
				}
				n.udpAddr, _ = net.ResolveUDPAddr("udp", n.service)
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
					pendingReq:   map[int64]*relayMsg{},
					set:          s,
					config:       r.config,
					mainQueue:    r.mainQueue,
				}
				n.udpAddr, _ = net.ResolveUDPAddr("udp", n.service)
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
}

type gate struct {
	config            *Config
	stopOnce          int32
	startOnce         int32
	listener          *cs.Listener
	totalPendingMsg   int64
	clients           map[*flynet.Socket]*flynet.Socket
	routeInfo         routeInfo
	queryingRouteInfo bool
	queryTimer        *time.Timer
	mainQueue         *queue.PriorityQueue
	serviceAddr       string
	seqCounter        int64
	pdToken           string
}

func (g *gate) queryRouteInfo() *sproto.QueryRouteInfoResp {
	pdService := strings.Split(g.config.PdService, ";")

	if len(pdService) == 0 {
		GetSugar().Fatalf("PdService is empty")
		return nil
	}

	okCh := make(chan *sproto.QueryRouteInfoResp)
	uu := make([]*flynet.Udp, len(pdService))

	for k, v := range pdService {
		go func(i int, remote string) {
			var localU *flynet.Udp
			var remoteAddr *net.UDPAddr
			var err error
			localU, err = flynet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
			if nil != err {
				GetSugar().Infof("%v", err)
				return
			}

			uu[i] = localU
			remoteAddr, err = net.ResolveUDPAddr("udp", remote)
			if nil != err {
				GetSugar().Infof("%v", err)
				return
			}

			req := &sproto.QueryRouteInfo{
				Service: g.serviceAddr,
				Token:   g.pdToken,
				Version: g.routeInfo.version,
			}

			for kk, _ := range g.routeInfo.sets {
				req.Sets = append(req.Sets, int32(kk))
			}

			localU.SendTo(remoteAddr, req)
			_, r, err := localU.ReadFrom(make([]byte, 1024*64))
			if nil == err {
				if resp, ok := r.(*sproto.QueryRouteInfoResp); ok {
					select {
					case okCh <- resp:
					default:
					}
				}
			}
		}(k, v)
	}

	ticker := time.NewTicker(3 * time.Second)

	var resp *sproto.QueryRouteInfoResp

	select {

	case v := <-okCh:
		resp = v
	case <-ticker.C:
	}
	ticker.Stop()

	for _, v := range uu {
		if nil != v {
			v.Close()
		}
	}

	return resp
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
		serviceAddr: service,
	}
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
				msg := v.(*relayMsg)
				msg.cli = session
				g.mainQueue.ForceAppend(0, msg)
			})
		})
	})
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (g *gate) mainLoop() {
	for {
		_, v := g.mainQueue.Pop()
		switch v.(type) {
		case *relayMsg:
			msg := v.(*relayMsg)
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

func (g *gate) startQueryTimer(timeout time.Duration) {
	g.queryTimer = time.AfterFunc(timeout, func() {
		go func() {
			if resp := g.queryRouteInfo(); nil != resp {
				g.routeInfo.onQueryRouteInfoResp(resp)
				g.startQueryTimer(time.Second * 10)
			} else {
				g.startQueryTimer(time.Millisecond * 10)
			}
		}()
	})
}

func (g *gate) Start() error {
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
			resp := g.queryRouteInfo()
			if nil != resp {
				g.routeInfo.onQueryRouteInfoResp(resp)
				break
			}
		}

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
		//关闭现有连接的读端，不会再接收新的req
		for _, v := range g.clients {
			v.ShutdownRead()
		}

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
