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

type set struct {
	setID  int
	nodes  map[int]*kvnode
	stores map[int]*store
}

type replyer struct {
	session         *flynet.Socket
	totalPendingReq *int64
}

func (r *replyer) reply(resp []byte) {
	atomic.AddInt64(r.totalPendingReq, -1)
	r.session.Send(resp)
}

func (r *replyer) replyErr(seqno int64, cmd uint16, err errcode.Error) {
	var sizeOfErrDesc int

	if nil != err && err.Code != 0 {
		sizeOfErrDesc = len(err.Desc)
		if sizeOfErrDesc > 0xFF {
			//描述超长，直接丢弃
			sizeOfErrDesc = cs.SizeErrDescLen
		} else {
			sizeOfErrDesc += cs.SizeErrDescLen
		}
	}

	payloadLen := cs.SizeSeqNo + cs.SizeCmd + cs.SizeErrCode + sizeOfErrDesc + cs.SizeCompress
	totalLen := cs.SizeLen + payloadLen
	if uint64(totalLen) > cs.MaxPacketSize {
		return
	}

	b := make([]byte, 0, totalLen)

	//写payload大小
	b = buffer.AppendUint32(b, uint32(payloadLen))
	//seqno
	b = buffer.AppendInt64(b, seqno)
	//cmd
	b = buffer.AppendUint16(b, cmd)
	//err
	b = buffer.AppendInt16(b, errcode.GetCode(err))

	if sizeOfErrDesc > 0 {
		b = buffer.AppendUint16(b, uint16(sizeOfErrDesc-cs.SizeErrDescLen))
		if sizeOfErrDesc > cs.SizeErrDescLen {
			b = buffer.AppendString(b, err.Desc)
		}
	}

	b = buffer.AppendByte(b, byte(0))

	r.reply(b)

}

func (r *replyer) dropReply() {
	atomic.AddInt64(r.totalPendingReq, -1)
}

type gate struct {
	cache
	config               *Config
	closed               int32
	closeCh              chan struct{}
	listener             *cs.Listener
	muC                  sync.Mutex
	clients              map[*flynet.Socket]*flynet.Socket
	mainQueue            *queue.PriorityQueue
	serviceAddr          string
	seqCounter           int64
	pdAddr               []*net.UDPAddr
	msgPerSecond         *movingAverage.MovingAverage //每秒客户端转发请求量的移动平均值
	msgRecv              int32
	totalPendingReq      int64
	SoftLimitReachedTime int64
	version              int64
	sets                 map[int]*set
	slotToStore          map[int]*store
}

func NewFlyGate(config *Config, service string) (*gate, error) {
	mainQueue := queue.NewPriorityQueue(2)

	if config.ReqLimit.SoftLimit <= 0 {
		config.ReqLimit.SoftLimit = 100000
	}

	if config.ReqLimit.HardLimit <= 0 {
		config.ReqLimit.HardLimit = 150000
	}

	if config.ReqLimit.SoftLimitSeconds <= 0 {
		config.ReqLimit.SoftLimitSeconds = 10
	}

	g := &gate{
		config:      config,
		clients:     map[*flynet.Socket]*flynet.Socket{},
		mainQueue:   mainQueue,
		sets:        map[int]*set{},
		slotToStore: map[int]*store{},
		cache: cache{
			l: list.New(),
		},
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

func (g *gate) checkKvnode(n *kvnode) bool {
	if s, ok := g.sets[n.setID]; ok {
		if _, ok = s.nodes[n.id]; ok {
			return true
		}
	}
	return false
}

func (g *gate) checkStore(st *store) bool {
	if s, ok := g.sets[st.setID]; ok {
		if _, ok = s.stores[st.id]; ok {
			return true
		}
	}
	return false
}

func (g *gate) getStore(store uint64) *store {
	setID := int(store >> 32)
	storeID := int(store & 0xFFFFFFFF)
	if s, ok := g.sets[setID]; ok {
		if store, ok := s.stores[storeID]; ok {
			return store
		}
	}
	return nil
}

func (g *gate) makeReplyer(session *flynet.Socket, req *forwordMsg) *replyer {
	replyer := &replyer{
		session:         session,
		totalPendingReq: &g.totalPendingReq,
	}

	if g.checkReqLimit(int(atomic.AddInt64(&g.totalPendingReq, 1))) {
		return replyer
	} else {
		replyer.replyErr(req.oriSeqno, req.cmd, errcode.New(errcode.Errcode_retry, "flykv busy,please retry later"))
		return nil
	}
}

func (g *gate) checkReqLimit(c int) bool {
	conf := g.config.ReqLimit

	if c > conf.HardLimit {
		return false
	}

	if c > conf.SoftLimit {
		nowUnix := time.Now().Unix()
		if !atomic.CompareAndSwapInt64(&g.SoftLimitReachedTime, 0, nowUnix) {
			SoftLimitReachedTime := atomic.LoadInt64(&g.SoftLimitReachedTime)
			if SoftLimitReachedTime > 0 && int(nowUnix-SoftLimitReachedTime) >= conf.SoftLimitSeconds {
				return false
			}
		}
	} else if SoftLimitReachedTime := atomic.LoadInt64(&g.SoftLimitReachedTime); SoftLimitReachedTime > 0 {
		atomic.CompareAndSwapInt64(&g.SoftLimitReachedTime, SoftLimitReachedTime, 0)
	}

	return true
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

var QueryRouteInfoDuration time.Duration = time.Millisecond * 1000

func doQueryRouteInfo(pdAddr []*net.UDPAddr, req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	if len(pdAddr) == 0 {
		GetSugar().Fatalf("PdService is empty")
		return nil
	}

	if resp, err := snet.UdpCall(pdAddr, req, &sproto.QueryRouteInfoResp{}, time.Second); nil == err {
		return resp.(*sproto.QueryRouteInfoResp)
	} else {
		return nil
	}
}

func (g *gate) onCliMsg(msg *forwordMsg) {
	s, ok := g.slotToStore[msg.slot]
	if !ok {
		g.addMsg(msg)
	} else {
		s.onCliMsg(msg)
	}
}

func (g *gate) paybackMsg(msg *forwordMsg) {
	now := time.Now()
	if msg.deadline.After(now) {
		g.onCliMsg(msg)
	} else {
		msg.dropReply()
	}
}

func (g *gate) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) {
	if nil == resp {
		return
	}

	if g.version == resp.Version {
		return
	}

	g.version = resp.Version

	for _, v := range resp.Sets {
		s, ok := g.sets[int(v.SetID)]
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
					setID:        int(v.SetID),
					gate:         g,
				}
				s.nodes[n.id] = n
			}

			for _, vv := range remove {
				n := s.nodes[int(vv)]
				delete(s.nodes, int(vv))
				if nil != n.session {
					n.session.Close(nil, 0)
				}
			}

		} else {
			s := &set{
				setID:  int(v.SetID),
				nodes:  map[int]*kvnode{},
				stores: map[int]*store{},
			}

			for _, vv := range v.Kvnodes {
				n := &kvnode{
					id:           int(vv.NodeID),
					service:      fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort),
					waittingSend: list.New(),
					waitResponse: map[int64]*forwordMsg{},
					setID:        int(v.SetID),
					gate:         g,
				}
				s.nodes[n.id] = n
			}

			for k, vv := range v.Stores {
				st := &store{
					id: int(vv),
					cache: cache{
						l: list.New(),
					},
					setID: int(v.SetID),
					gate:  g,
				}
				st.slots, _ = bitmap.CreateFromJson(v.Slots[k])
				s.stores[st.id] = st
			}
			g.sets[s.setID] = s
		}
	}

	for _, v := range resp.RemoveSets {
		if s, ok := g.sets[int(v)]; ok {
			for _, vv := range s.nodes {
				if nil != vv.session {
					vv.session.Close(nil, 0)
				}
			}
			for _, vv := range s.stores {
				//将待转发请求回收
				for vvv := vv.l.Front(); nil != vvv; vvv = vv.l.Front() {
					msg := vvv.Value.(*forwordMsg)
					vv.removeMsg(msg)
					g.addMsg(msg)
				}
			}
			delete(g.sets, int(v))
		}
	}

	g.slotToStore = map[int]*store{}
	for _, v := range g.sets {
		for _, vv := range v.stores {
			slots := vv.slots.GetOpenBits()
			for _, vvv := range slots {
				g.slotToStore[vvv] = vv
			}
		}
	}

	//路由更新，尝试处理没有正确路由信息的请求
	size := g.lenMsg()
	now := time.Now()
	for i := 0; i < size; i++ {
		msg := g.l.Front().Value.(*forwordMsg)
		g.removeMsg(msg)
		if msg.deadline.After(now) {
			g.onCliMsg(msg)
		} else {
			msg.dropReply()
		}
	}
}

func (g *gate) queryRouteInfo() {
	req := &sproto.QueryRouteInfo{
		Version: g.version,
	}

	for kk, _ := range g.sets {
		req.Sets = append(req.Sets, int32(kk))
	}

	go func() {
		resp := doQueryRouteInfo(g.pdAddr, req)
		g.callInQueue(1, func() {
			g.onQueryRouteInfoResp(resp)
			delay := QueryRouteInfoDuration
			if g.lenMsg() > 0 {
				delay = time.Millisecond * 50
			}
			g.startQueryTimer(delay)
		})
	}()
}

func (g *gate) startQueryTimer(delay time.Duration) {
	time.AfterFunc(delay, func() {
		g.queryRouteInfo()
	})
}

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		g.muC.Lock()
		g.clients[session] = session
		g.muC.Unlock()
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
			if replyer := g.makeReplyer(session, msg); nil != replyer {
				msg.replyer = replyer
				g.mainQueue.ForceAppend(0, msg)
			}
		})
	}, g.onScanner)
	GetSugar().Infof("flygate start on %s", g.serviceAddr)
}

func (g *gate) mainLoop() {
	for {
		_, v := g.mainQueue.Pop()
		switch v.(type) {
		case *forwordMsg:
			msg := v.(*forwordMsg)
			if msg.slot >= 0 && msg.slot < slot.SlotCount {
				msg.deadlineTimer.set(g.afterFunc(msg.deadline.Sub(time.Now()), msg.dropReply))
				g.seqCounter++
				msg.seqno = g.seqCounter
				g.onCliMsg(msg)
			}
		case func():
			v.(func())()
		}
	}
}

func (g *gate) start() error {
	var err error

	g.listener, err = cs.NewListener("tcp", g.serviceAddr, flynet.OutputBufLimit{
		OutPutLimitSoft:        1024 * 1024 * 10,
		OutPutLimitSoftSeconds: 10,
		OutPutLimitHard:        1024 * 1024 * 50,
	})

	if nil != err {
		return err
	}

	for {
		resp := doQueryRouteInfo(g.pdAddr, &sproto.QueryRouteInfo{})
		if nil != resp {
			g.onQueryRouteInfoResp(resp)
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
			return atomic.LoadInt64(&g.totalPendingReq) == 0
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
