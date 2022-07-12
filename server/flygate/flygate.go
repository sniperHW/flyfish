package flygate

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flynet "github.com/sniperHW/flyfish/pkg/net"
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

type containerElementList struct {
	listElement *list.Element
	c           container
}

func (e *containerElementList) remove() {
	e.c.remove(e)
}

func (e *containerElementList) container() container {
	return e.c
}

type containerList struct {
	l *list.List
}

func (c *containerList) add(r *request) {
	e := &containerElementList{c: c}
	e.listElement = c.l.PushBack(r)
	r.containerElements = append(r.containerElements, e)
}

func (c *containerList) remove(e containerElement) {
	if el, ok := e.(*containerElementList); ok {
		request := c.l.Remove(el.listElement).(*request)
		for i, v := range request.containerElements {
			if v == e {
				request.containerElements[i] = request.containerElements[len(request.containerElements)-1]
				request.containerElements = request.containerElements[:len(request.containerElements)-1]
				break
			}
		}
	}
}

type containerElementMap struct {
	request *request
	c       container
}

func (e *containerElementMap) remove() {
	e.c.remove(e)
}

func (e *containerElementMap) container() container {
	return e.c
}

type containerMap struct {
	m map[int64]*request
}

func (c *containerMap) add(r *request) {
	e := &containerElementMap{request: r, c: c}
	c.m[r.seqno] = r
	r.containerElements = append(r.containerElements, e)
}

func (c *containerMap) remove(e containerElement) {
	if em, ok := e.(*containerElementMap); ok {
		request := em.request
		delete(c.m, request.seqno)
		for i, v := range request.containerElements {
			if v == e {
				request.containerElements[i] = request.containerElements[len(request.containerElements)-1]
				request.containerElements = request.containerElements[:len(request.containerElements)-1]
				break
			}
		}
	}
}

type gate struct {
	sync.Mutex
	config               *Config
	closed               int32
	closeCh              chan struct{}
	listener             *cs.Listener
	clients              map[*flynet.Socket]*flynet.Socket
	serviceAddr          string
	pdAddr               []*net.UDPAddr
	SoftLimitReachedTime int64
	version              int64
	sets                 map[int]*set
	slotToStore          map[int]*store
	waitResp             containerMap
	waitSend             containerList
	scannerCount         int32
}

func (g *gate) checkKvnode(n *kvnode) bool {
	if s, ok := g.sets[n.setID]; ok {
		if node, ok := s.nodes[n.id]; ok {
			return n == node
		}
	}
	return false
}

func (g *gate) checkStore(st *store) bool {
	if s, ok := g.sets[st.setID]; ok {
		if store, ok := s.stores[st.id]; ok {
			return store == st
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

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		g.Lock()
		g.clients[session] = session
		g.Unlock()
		session.SetInBoundProcessor(NewCliReqInboundProcessor())
		session.SetRecvTimeout(flyproto.PingTime * 10)

		session.SetCloseCallBack(func(session *flynet.Socket, reason error) {
			g.Lock()
			delete(g.clients, session)
			g.Unlock()
		})

		session.BeginRecv(func(session *flynet.Socket, v interface{}) {
			if atomic.LoadInt32(&g.closed) == 1 {
				//服务关闭不再接受新新的请求
				return
			}
			if v.(*request).slot >= 0 && v.(*request).slot < slot.SlotCount {
				g.Lock()
				var req *request
				if g.checkReqLimit(len(g.waitResp.m) + 1) {
					if req = g.waitResp.m[v.(*request).seqno]; nil != req {
						//相同seqno的请求已经存在，只reply最后的请求
						req.from = session
					} else {
						req = v.(*request)
						req.from = session
						req.deadlineTimer = time.AfterFunc(req.deadline.Sub(time.Now()), func() {
							g.Lock()
							req.dropReply()
							g.Unlock()
						})
						g.waitResp.add(req)
						store, ok := g.slotToStore[req.slot]
						if !ok {
							g.waitSend.add(req)
						} else {
							g.storeSend(store, req, time.Now())
						}
					}
				} else {
					req = v.(*request)
					session.Send(makeErrResponse(req.seqno, req.cmd(), errcode.New(errcode.Errcode_retry, "flykv busy,please retry later")))
				}
				g.Unlock()
			}
		})
	}, g.onScanner)
	GetSugar().Infof("flygate start on %s", g.serviceAddr)
}

var QueryRouteInfoDuration time.Duration = time.Millisecond * 1000

func doQueryRouteInfo(pdAddr []*net.UDPAddr, req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	if resp, err := snet.UdpCall(pdAddr, req, &sproto.QueryRouteInfoResp{}, time.Second); nil == err {
		return resp.(*sproto.QueryRouteInfoResp)
	} else {
		return nil
	}
}

func (g *gate) onQueryRouteInfoResp(resp *sproto.QueryRouteInfoResp) time.Duration {
	if nil == resp {
		return QueryRouteInfoDuration
	}

	if atomic.LoadInt64(&g.version) == resp.Version {
		return QueryRouteInfoDuration
	}

	atomic.StoreInt64(&g.version, resp.Version)

	var localSets []*set

	g.Lock()
	defer g.Unlock()

	for _, v := range g.sets {
		localSets = append(localSets, v)
	}

	sort.Slice(localSets, func(i, j int) bool {
		return localSets[i].setID < localSets[j].setID
	})

	sort.Slice(resp.Sets, func(i, j int) bool {
		return resp.Sets[i].SetID < resp.Sets[j].SetID
	})

	var removeSets []*set
	var addSets []*sproto.RouteInfoSet

	i := 0
	j := 0

	for i < len(localSets) && j < len(resp.Sets) {
		if localSets[i].setID == int(resp.Sets[j].SetID) {

			if len(resp.Sets[j].Stores) > 0 {
				v := resp.Sets[j]
				s := localSets[i]

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
						waitSend: containerList{l: list.New()},
						waitResp: containerMap{m: map[int64]*request{}},
						setID:    int(v.SetID),
					}
					s.nodes[n.id] = n
				}

				for _, vv := range remove {
					kvnode := s.nodes[int(vv)]
					delete(s.nodes, int(vv))
					if nil != kvnode.session {
						kvnode.session.Close(nil, 0)
					} else {
						for v := kvnode.waitSend.l.Front(); nil != v; v = kvnode.waitSend.l.Front() {
							request := v.Value.(*request)
							request.remove(&kvnode.waitSend)
							g.waitSend.add(request)
						}
					}

					for _, store := range s.stores {
						if store.leader == kvnode {
							store.leader = nil
						}
					}
				}
			}

			i++
			j++
		} else if localSets[i].setID > int(resp.Sets[j].SetID) {
			addSets = append(addSets, resp.Sets[j])
			j++
		} else {
			removeSets = append(removeSets, localSets[i])
			i++
		}
	}

	if len(localSets[i:]) > 0 {
		removeSets = append(removeSets, localSets[i:]...)
	}

	if len(resp.Sets[j:]) > 0 {
		addSets = append(addSets, resp.Sets[j:]...)
	}

	for _, v := range addSets {
		s := &set{
			setID:  int(v.SetID),
			nodes:  map[int]*kvnode{},
			stores: map[int]*store{},
		}

		for _, vv := range v.Kvnodes {
			n := &kvnode{
				id:       int(vv.NodeID),
				service:  fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort),
				waitSend: containerList{l: list.New()},
				waitResp: containerMap{m: map[int64]*request{}},
				setID:    int(v.SetID),
			}
			s.nodes[n.id] = n
		}

		for k, vv := range v.Stores {
			st := &store{
				id:       int(vv),
				waitSend: containerList{l: list.New()},
				setID:    int(v.SetID),
			}
			st.slots, _ = bitmap.CreateFromJson(v.Slots[k])
			s.stores[st.id] = st
		}
		g.sets[s.setID] = s

		GetSugar().Infof("add set %d %v", s.setID, s.stores)

	}

	for _, s := range removeSets {
		delete(g.sets, s.setID)
		for _, kvnode := range s.nodes {
			if nil != kvnode.session {
				kvnode.session.Close(nil, 0)
			} else {
				for v := kvnode.waitSend.l.Front(); nil != v; v = kvnode.waitSend.l.Front() {
					request := v.Value.(*request)
					request.remove(&kvnode.waitSend)
					g.waitSend.add(request)
				}
			}
		}
		for _, store := range s.stores {
			//将待转发请求回收
			for v := store.waitSend.l.Front(); nil != v; v = store.waitSend.l.Front() {
				request := v.Value.(*request)
				request.remove(&store.waitSend)
				g.waitSend.add(request)
			}
		}
	}

	g.slotToStore = map[int]*store{}
	for _, set := range g.sets {
		for _, store := range set.stores {
			slots := store.slots.GetOpenBits()
			for _, vvv := range slots {
				g.slotToStore[vvv] = store
			}
		}
	}

	//路由更新，尝试处理没有正确路由信息的请求
	now := time.Now()
	cur := g.waitSend.l.Front()
	for nil != cur {
		request := cur.Value.(*request)
		next := cur.Next()
		if request.deadline.After(now) {
			if store, ok := g.slotToStore[request.slot]; ok {
				request.remove(&g.waitSend)
				g.storeSend(store, request, now)
			}
		} else {
			request.dropReply()
			request.remove(&g.waitSend)
		}
		cur = next
	}

	if g.waitSend.l.Len() > 0 {
		return time.Millisecond * 50
	} else {
		return QueryRouteInfoDuration
	}
}

func (g *gate) queryRouteInfo() {
	resp := doQueryRouteInfo(g.pdAddr, &sproto.QueryRouteInfo{
		Version: atomic.LoadInt64(&g.version),
	})

	if atomic.LoadInt32(&g.closed) == 0 {
		time.AfterFunc(g.onQueryRouteInfoResp(resp), g.queryRouteInfo)
	}
}

func NewFlyGate(config *Config, service string) (*gate, error) {

	if config.ReqLimit.SoftLimit <= 0 {
		config.ReqLimit.SoftLimit = 100000
	}

	if config.ReqLimit.HardLimit <= 0 {
		config.ReqLimit.HardLimit = 150000
	}

	if config.ReqLimit.SoftLimitSeconds <= 0 {
		config.ReqLimit.SoftLimitSeconds = 10
	}

	if config.MaxScannerCount <= 0 {
		config.MaxScannerCount = 100
	}

	g := &gate{
		config:      config,
		clients:     map[*flynet.Socket]*flynet.Socket{},
		sets:        map[int]*set{},
		slotToStore: map[int]*store{},
		serviceAddr: service,
		closeCh:     make(chan struct{}),
		waitSend:    containerList{l: list.New()},
		waitResp:    containerMap{m: map[int64]*request{}},
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

	return g, err
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
				GateService: g.serviceAddr,
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

	g.startListener()

	time.AfterFunc(QueryRouteInfoDuration, func() {
		g.queryRouteInfo()
	})

	return nil
}

func waitCondition(fn func() bool) {
	waitCh := make(chan struct{})
	go func() {
		for {
			if fn() {
				select {
				case waitCh <- struct{}{}:
				default:
				}
				break
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()
	<-waitCh
}

func (g *gate) Stop() {
	if atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		g.listener.Close()

		close(g.closeCh)

		//等待所有消息处理完
		waitCondition(func() bool {
			g.Lock()
			defer g.Unlock()
			return len(g.waitResp.m) == 0
		})

		g.Lock()
		for _, v := range g.clients {
			go v.Close(nil, time.Second*5)
		}
		g.Unlock()

		waitCondition(func() bool {
			g.Lock()
			defer g.Unlock()
			return len(g.clients) == 0
		})

		waitCondition(func() bool {
			return atomic.LoadInt32(&g.scannerCount) == 0
		})
	}
}
