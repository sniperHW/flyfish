package client

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/movingAverage"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"math/rand"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ClientTimeout uint32 = 6000 //6sec
var maxPendingSize int = 10000

var seqno int64

var outputBufLimit flynet.OutputBufLimit = flynet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

type EventQueueI interface {
	Post(priority int, fn interface{}, args ...interface{}) error
}

func formatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

func Recover() {
	if r := recover(); r != nil {
		buf := make([]byte, 65535)
		l := runtime.Stack(buf, false)
		GetSugar().Errorf(formatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
	}
}

type ClientConf struct {
	CallbackQueue   EventQueueI //响应回调的事件队列
	CBEventPriority int         //回调事件优先级
	//cluster模式
	PD []string //pd服务地址
	//solo模式
	UnikeyPlacement func(string) int //返回unikey所在的store,对于连接proxy的方式无需提供,store字段由proxy填写
	SoloService     string
	Stores          []int
	FetchRowCount   int //scanner一次从服务器获取的最大行数量，如果行数据比较大应将此值设小一点，避免数据包超过大小限制
}

func makeWaitResp() *map[int64]*cmdContext {
	waitResp := map[int64]*cmdContext{}
	return &waitResp
}

type serverConn struct {
	mu              *sync.Mutex
	service         string
	session         *flynet.Socket
	pendingSend     *list.List             //因为连接尚未建立被排队等待发送的请求
	waitResp        *map[int64]*cmdContext //已经发送等待对端应答的请求
	connecting      int32
	closed          *int32
	UnikeyPlacement func(string) int
	c               *Client
	removed         bool
}

func (this *serverConn) onDisconnected() {
	this.mu.Lock()
	this.session = nil
	waitResp := *this.waitResp
	this.waitResp = makeWaitResp()

	for _, v := range waitResp {
		if nil != v.deadlineTimer {
			v.deadlineTimer.Stop()
			v.deadlineTimer = nil
		}
	}

	this.mu.Unlock()

	for _, v := range waitResp {
		v.doCallBack(v.unikey, v.cb, errcode.New(errcode.Errcode_error, "lose connection"))
		releaseCmdContext(v)
	}
}

func (this *serverConn) onConnected(session *flynet.Socket) {

	this.mu.Lock()
	defer this.mu.Unlock()

	atomic.StoreInt32(&this.connecting, 0)
	this.session = session
	this.session.SetInBoundProcessor(cs.NewRespInboundProcessor())
	this.session.SetEncoder(&cs.ReqEncoder{})
	this.session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
		GetSugar().Infof("socket close %v", reason)
		go this.onDisconnected()
	}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
		this.onMessage(msg.(*cs.RespMessage))
	})

	GetSugar().Infof("connect to flygate ok")

	now := time.Now()
	//发送被排队的请求
	for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
		e := this.pendingSend.Remove(v).(*cmdContext)
		e.listElement = nil
		e.l = nil
		this.sendReq(e, now)
	}
}

func (this *serverConn) sendReq(c *cmdContext, now time.Time) {
	if c.req.Timeout = uint32(c.deadline.Sub(time.Now()) / time.Millisecond); c.req.Timeout > 0 {
		if nil != this.UnikeyPlacement {
			//如果提供了定位器，使用定位器直接计算出Store
			c.req.Store = this.UnikeyPlacement(c.req.UniKey)
		}
		(*this.waitResp)[c.req.Seqno] = c
		c.waitResp = this.waitResp
		this.session.Send(c.req)
	}
}

func (this *serverConn) exec(c *cmdContext) errcode.Error {

	var errCode errcode.Error

	c.serverConn = this

	if nil != this.session {
		this.sendReq(c, time.Now())
	} else {
		this.connect()
		if this.pendingSend.Len() < maxPendingSize {
			c.l = this.pendingSend
			c.listElement = this.pendingSend.PushBack(c)
		} else {
			errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
		}
	}

	return errCode
}

func (this *serverConn) connect() {
	if atomic.CompareAndSwapInt32(&this.connecting, 0, 1) {
		go func() {
			ok := false
			for {
				if session, err := cs.NewConnector("tcp", this.service, outputBufLimit).Dial(time.Second * 1); nil == err {
					this.onConnected(session)
					ok = true
				}

				if ok {
					return
				} else {
					/*
					 * solo模式只有一个地址，只能一直尝试连接
					 * cluster模式可能有多个地址，如果当前地址连接不上可以尝试换一个地址，如果无法更换再继续尝试连接
					 */
					if this.c.conf.SoloService != "" || !this.c.onConnectFailed(this) {
						time.Sleep(100 * time.Millisecond)
						this.mu.Lock()
						if atomic.LoadInt32(this.closed) == 1 || this.pendingSend.Len() == 0 {
							atomic.StoreInt32(&this.connecting, 0)
							this.mu.Unlock()
							return
						} else {
							this.mu.Unlock()
						}
					} else {
						atomic.StoreInt32(&this.connecting, 0)
						return
					}
				}
			}
		}()
	}
}

type Client struct {
	mu            sync.Mutex
	conf          ClientConf
	closed        int32
	serverConnMap map[string]*serverConn
	usedConn      *serverConn
	pendingSend   *list.List //usedConn==nil时被排队等待发送的请求
	pdAddr        []*net.UDPAddr
	msgPerSecond  *movingAverage.MovingAverage
	msgSend       int32
	gates         []*sproto.Flygate
}

func (this *Client) callcb(unikey string, cb callback, a interface{}) {
	switch a.(type) {
	case errcode.Error:
		cb.onError(unikey, a.(errcode.Error))
	default:
		cb.onResult(unikey, a)
	}
}

func (this *Client) doCallBack(unikey string, cb callback, a interface{}) {
	cbqueue := this.conf.CallbackQueue
	priority := this.conf.CBEventPriority

	if nil != cbqueue && cb.sync == false {
		cbqueue.Post(priority, this.callcb, unikey, cb, a)
	} else {
		defer Recover()
		this.callcb(unikey, cb, a)
	}
}

func QueryGate(pd []*net.UDPAddr, timeout time.Duration) (ret []*sproto.Flygate) {
	context := snet.MakeUniqueContext()
	if resp := snet.UdpCall(pd, snet.MakeMessage(context, &sproto.GetFlyGateList{}), timeout, func(respCh chan interface{}, r interface{}) {
		if m, ok := r.(*snet.Message); ok {
			if resp, ok := m.Msg.(*sproto.GetFlyGateListResp); ok && context == m.Context {
				select {
				case respCh <- resp.List:
				default:
				}
			}
		}
	}); nil != resp {
		ret = resp.([]*sproto.Flygate)
	}
	return
}

func (this *Client) onConnectFailed(conn *serverConn) bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	var ret bool
	if this.usedConn != conn {
		ret = true
	} else if len(this.serverConnMap) == 1 {
		ret = false
	} else {
		conns := []*serverConn{}
		for k, v := range this.serverConnMap {
			if k != conn.service {
				conns = append(conns, v)
			}
		}
		this.usedConn = conns[int(rand.Int31())%len(conns)]
	}

	if ret {
		for v := conn.pendingSend.Front(); v != nil; v = conn.pendingSend.Front() {
			e := conn.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil

			e.serverConn = this.usedConn

			if nil != this.usedConn.session {
				this.usedConn.sendReq(e, time.Now())
			} else {
				this.usedConn.connect()
				e.l = this.usedConn.pendingSend
				e.listElement = this.usedConn.pendingSend.PushBack(e)
			}
		}
	}

	return ret
}

func (this *Client) exec(c *cmdContext) {
	var errCode errcode.Error

	c.doCallBack = this.doCallBack

	this.mu.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		errCode = errcode.New(errcode.Errcode_error, "client closed")
	} else {
		if nil == this.usedConn && this.pendingSend.Len() >= maxPendingSize {
			errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
		} else {
			c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
			c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
			if nil != this.usedConn {
				errCode = this.usedConn.exec(c)
			} else {
				c.l = this.pendingSend
				c.listElement = this.pendingSend.PushBack(c)
			}
		}
	}

	this.mu.Unlock()

	if errCode != nil {
		this.doCallBack(c.unikey, c.cb, errCode)
		releaseCmdContext(c)
	} else {
		atomic.AddInt32(&this.msgSend, 1)
	}
}

func (this *Client) Close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.mu.Lock()
		defer this.mu.Unlock()
		if "" != this.conf.SoloService {
			if nil != this.usedConn.session {
				this.usedConn.session.Close(nil, 0)
			}
		} else {
			this.usedConn = nil
			for _, v := range this.serverConnMap {
				v.session.Close(nil, 0)
			}
		}
	}
}

func (this *Client) onGates(gates []*sproto.Flygate) {
	this.mu.Lock()
	localGates := []string{}
	for k, _ := range this.serverConnMap {
		localGates = append(localGates, k)
	}
	this.mu.Unlock()

	sort.Slice(localGates, func(i, j int) bool {
		return localGates[i] < localGates[j]
	})

	sort.Slice(gates, func(i, j int) bool {
		return gates[i].Service < gates[j].Service
	})

	add := []*sproto.Flygate{}
	remove := []string{}

	i := 0
	j := 0

	for i < len(gates) && j < len(localGates) {
		if gates[i].Service == localGates[j] {
			i++
			j++
		} else if gates[i].Service > localGates[j] {
			remove = append(remove, localGates[j])
			j++
		} else {
			add = append(add, gates[i])
			i++
		}
	}

	if len(gates[i:]) > 0 {
		add = append(add, gates[i:]...)
	}

	if len(localGates[j:]) > 0 {
		remove = append(remove, localGates[j:]...)
	}

	this.mu.Lock()

	for _, v := range add {
		conn := &serverConn{
			mu:          &this.mu,
			service:     v.Service,
			pendingSend: list.New(),
			waitResp:    makeWaitResp(),
			closed:      &this.closed,
			c:           this,
		}
		this.serverConnMap[v.Service] = conn
	}

	for _, v := range remove {
		conn := this.serverConnMap[v]

		delete(this.serverConnMap, v)

		if nil != this.usedConn && v == this.usedConn.service {
			this.usedConn = nil
		}

		if len(*conn.waitResp) == 0 {
			if nil != conn.session {
				conn.session.Close(nil, 0)
			}
		} else {
			conn.removed = true
		}
	}

	this.gates = gates

	if nil == this.usedConn {
		this.usedConn = this.serverConnMap[gates[int(rand.Int31())%len(gates)].Service]
		now := time.Now()
		for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
			e := this.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil

			if nil != this.usedConn.session {
				this.usedConn.sendReq(e, now)
			} else {
				this.usedConn.connect()
				e.l = this.usedConn.pendingSend
				e.listElement = this.usedConn.pendingSend.PushBack(e)
			}
		}
	} else {
		this.tryGateBalance()
	}

	this.mu.Unlock()
}

func (this *Client) changeFlygate(newGate string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if nil != this.usedConn && this.usedConn.service == newGate {
		return
	}

	g := this.serverConnMap[newGate]

	if nil == g {
		return
	}

	this.usedConn = g
}

func (this *Client) tryGateBalance() {
	var current *sproto.Flygate
	average := 0
	for _, v := range this.gates {
		average += int(v.MsgPerSecond)
		if v.Service == this.usedConn.service {
			current = v
		}
	}
	average /= len(this.gates)

	msgSendPerSend := this.msgPerSecond.GetAverage()

	if nil != current && int(current.MsgPerSecond)-msgSendPerSend > average {
		go func() {
			req := &sproto.ChangeFlyGate{CurrentGate: current.Service, MsgSendPerSecond: int32(msgSendPerSend)}
			context := snet.MakeUniqueContext()
			if resp := snet.UdpCall(this.pdAddr, snet.MakeMessage(context, req), time.Second, func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.ChangeFlyGateResp); ok && context == m.Context {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			}); nil != resp {
				if ret := resp.(*sproto.ChangeFlyGateResp); ret.Ok {
					this.changeFlygate(ret.Service)
				}
			}
		}()
	}

}

func (this *Client) queryRouteInfo() {
	go func() {
		gates := QueryGate(this.pdAddr, time.Second)

		if atomic.LoadInt32(&this.closed) == 1 {
			return
		}

		timeout := time.Millisecond * 100
		if len(gates) > 0 {
			this.onGates(gates)
			timeout = time.Second * 5
		}

		time.AfterFunc(timeout, func() {
			this.queryRouteInfo()
		})
	}()
}

func (this *Client) refreshMsgPerSecond() {
	if atomic.LoadInt32(&this.closed) == 0 {
		this.msgPerSecond.Add(int(atomic.LoadInt32(&this.msgSend)))
		atomic.StoreInt32(&this.msgSend, 0)
		time.AfterFunc(time.Second, this.refreshMsgPerSecond)
	}
}

func OpenClient(conf ClientConf) (*Client, error) {
	if "" == conf.SoloService && len(conf.PD) == 0 {
		return nil, errors.New("cluster mode,but pd empty")
	} else {

		c := &Client{
			conf:         conf,
			msgPerSecond: movingAverage.New(5),
		}

		if "" != conf.SoloService {
			c.usedConn = &serverConn{
				mu:              &c.mu,
				service:         conf.SoloService,
				pendingSend:     list.New(),
				waitResp:        makeWaitResp(),
				UnikeyPlacement: conf.UnikeyPlacement,
				closed:          &c.closed,
				c:               c,
			}
		} else {
			for _, v := range conf.PD {
				if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
					c.pdAddr = append(c.pdAddr, addr)
				}
			}

			if len(c.pdAddr) == 0 {
				return nil, errors.New("pd is empty")
			}

			c.pendingSend = list.New()
			c.serverConnMap = map[string]*serverConn{}
			c.queryRouteInfo()
		}
		c.refreshMsgPerSecond()
		return c, nil
	}
}
