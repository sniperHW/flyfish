package client

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
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

type ClientType int

const (
	ClientType_FlyKv  = ClientType(1) //请求发往flykv,由缓存来实现数据的读写
	ClientType_FlySql = ClientType(2) //请求发往flysql,直接交给数据库完成数据读写
)

type SoloConf struct {
	Service         string
	UnikeyPlacement func(string) int //返回unikey所在的store,对于连接proxy的方式无需提供,store字段由proxy填写
	Stores          []int
}

type ClusterConf struct {
	PD []string //pd服务地址
}

type ClientConf struct {
	CallbackQueue   EventQueueI //响应回调的事件队列
	CBEventPriority int         //回调事件优先级
	FetchRowCount   int         //scanner一次从服务器获取的最大行数量，如果行数据比较大应将此值设小一点，避免数据包超过大小限制
	ClientType      ClientType
	SoloConf        *SoloConf
	ClusterConf     *ClusterConf
}

type serverConn struct {
	service         string
	session         *flynet.Socket
	pendingSend     *list.List            //因为连接尚未建立被排队等待发送的请求
	waitResp        map[int64]*cmdContext //已经发送等待对端应答的请求
	connecting      bool
	UnikeyPlacement func(string) int
	c               *Client
	removed         bool
}

func (this *serverConn) onDisconnected() {
	this.c.mu.Lock()
	this.session = nil

	ctxs := []*cmdContext{}
	for _, v := range this.waitResp {
		delete(this.waitResp, v.req.Seqno)
		v.waitResp = nil
		ctxs = append(ctxs, v)
	}
	this.c.mu.Unlock()

	for _, v := range ctxs {
		this.c.doCallBack(v, errcode.New(errcode.Errcode_error, "lose connection"))
	}
}

func (this *serverConn) connect() {
	session, err := cs.NewConnector("tcp", this.service, outputBufLimit).Dial(time.Second * 1)
	this.c.mu.Lock()
	if this.c.closed {
		ctxs := []*cmdContext{}
		for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
			e := this.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil
			ctxs = append(ctxs, e)
		}
		if nil != session {
			session.Close(nil, 0)
		}
		this.c.mu.Unlock()
		for _, c := range ctxs {
			this.c.doCallBack(c, errcode.New(errcode.Errcode_error, "client closed"))
		}

	} else if nil == err {
		this.connecting = false
		this.session = session
		this.session.SetInBoundProcessor(cs.NewRespInboundProcessor())
		this.session.SetEncoder(&cs.ReqEncoder{})
		this.session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
			GetSugar().Infof("socket close %v", reason)
			go this.onDisconnected()
		}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
			this.onMessage(msg.(*cs.RespMessage))
		})

		now := time.Now()
		//发送被排队的请求
		for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
			e := this.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil
			this.sendReq(e, now)
		}
		this.c.mu.Unlock()
	} else if this.removed {
		//当前服务已经被移除，尝试通过其它可用服务发送请求
		this.connecting = false
		for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
			e := this.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil
			if avaliableConn := this.c.getAvaliable(); nil != avaliableConn {
				avaliableConn.exec(e)
			} else {
				e.l = this.c.pendingSend
				e.listElement = this.c.pendingSend.PushBack(e)
			}
		}
		this.c.mu.Unlock()
	} else {
		this.c.mu.Unlock()
		time.AfterFunc(1000*time.Millisecond, this.connect)
	}
}

func (this *serverConn) sendReq(c *cmdContext, now time.Time) {
	if c.req.Timeout = uint32(c.deadline.Sub(time.Now()) / time.Millisecond); c.req.Timeout > 0 {
		if nil != this.UnikeyPlacement {
			//如果提供了定位器，使用定位器直接计算出Store
			c.req.Store = this.UnikeyPlacement(c.req.UniKey)
		}
		this.waitResp[c.req.Seqno] = c
		c.waitResp = &this.waitResp
		this.session.Send(c.req)
	}
}

func (this *serverConn) exec(c *cmdContext) {
	if nil != this.session {
		this.sendReq(c, time.Now())
	} else {
		c.l = this.pendingSend
		c.listElement = this.pendingSend.PushBack(c)
		if !this.connecting {
			this.connecting = true
			go this.connect()
		}
	}
}

type Client struct {
	mu             sync.Mutex
	conf           ClientConf
	closed         bool
	pendingSend    *list.List //len(avaliableConns)==0时被排队等待发送的请求
	pendingCount   int32
	seqno          int64
	avaliableConns []*serverConn
	dir            dir
}

func (this *Client) callcb(ctx *cmdContext, a interface{}) {
	switch a.(type) {
	case errcode.Error:
		ctx.cb.onError(ctx.unikey, a.(errcode.Error))
	default:
		ctx.cb.onResult(ctx.unikey, a)
	}
}

func (this *Client) doCallBack(ctx *cmdContext, a interface{}) {
	if atomic.CompareAndSwapInt32(&ctx.cb.emmited, 0, 1) {
		atomic.AddInt32(&this.pendingCount, -1)
		//如果a.(type) == result说明是通过serverConn.onMessage进来的，无需再执行清理
		if _, ok := a.(errcode.Error); ok {
			this.mu.Lock()
			if nil != ctx.deadlineTimer {
				ctx.deadlineTimer.Stop()
			}

			if nil != ctx.waitResp {
				delete(*ctx.waitResp, ctx.req.Seqno)
			}

			if nil != ctx.listElement {
				ctx.l.Remove(ctx.listElement)
			}

			this.mu.Unlock()
		}

		cbqueue := this.conf.CallbackQueue
		priority := this.conf.CBEventPriority

		if nil != cbqueue && ctx.cb.sync == false {
			cbqueue.Post(priority, this.callcb, ctx, a)
		} else {
			defer Recover()
			this.callcb(ctx, a)
		}
	}
}

func (this *Client) getAvaliable() *serverConn {
	if len(this.avaliableConns) > 0 {
		return this.avaliableConns[int(rand.Int31())%len(this.avaliableConns)]
	} else {
		return nil
	}
}

func (this *Client) reExec(c *cmdContext) {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		this.doCallBack(c, errcode.New(errcode.Errcode_error, "client closed"))
	} else {
		if avaliableConn := this.getAvaliable(); nil != avaliableConn {
			avaliableConn.exec(c)
		} else {
			c.l = this.pendingSend
			c.listElement = this.pendingSend.PushBack(c)
		}
		this.mu.Unlock()
	}
}

func (this *Client) exec(c *cmdContext) {
	pendingCount := atomic.AddInt32(&this.pendingCount, 1)
	var errCode errcode.Error

	this.mu.Lock()
	if this.closed {
		errCode = errcode.New(errcode.Errcode_error, "client closed")
	} else {
		avaliableConn := this.getAvaliable()
		if nil == avaliableConn {
			if pendingCount > int32(maxPendingSize) {
				errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
			} else {
				c.l = this.pendingSend
				c.listElement = this.pendingSend.PushBack(c)
			}
		} else {
			avaliableConn.exec(c)
		}
	}

	if nil == errCode {
		if nil == c.deadlineTimer {
			c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
			c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
		}
	} else {
		go this.doCallBack(c, errCode)
	}
	this.mu.Unlock()
}

func (this *Client) Close() {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return
	} else {
		this.closed = true
		for _, v := range this.avaliableConns {
			if nil != v.session {
				v.session.Close(nil, 0)
			}
		}
	}
}

func OpenClient(conf ClientConf) (*Client, error) {

	if !(conf.ClientType == ClientType_FlyKv || conf.ClientType == ClientType_FlySql) {
		return nil, errors.New("invaild ClientType")
	} else if nil == conf.SoloConf && nil == conf.ClusterConf {
		return nil, errors.New("SoloConf and ClusterConf is nil")
	} else {
		c := &Client{
			conf: conf,
		}

		if nil != conf.SoloConf {
			c.avaliableConns = append(c.avaliableConns, &serverConn{
				service:         conf.SoloConf.Service,
				pendingSend:     list.New(),
				waitResp:        map[int64]*cmdContext{},
				UnikeyPlacement: conf.SoloConf.UnikeyPlacement,
				c:               c,
			})
		} else {
			if len(conf.ClusterConf.PD) == 0 {
				return nil, errors.New("PD is empty")
			} else {
				var pdAddr []*net.UDPAddr

				for _, v := range conf.ClusterConf.PD {
					if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
						pdAddr = append(pdAddr, addr)
					}
				}

				if len(pdAddr) == 0 {
					return nil, errors.New("pd is empty")
				}

				if conf.ClientType == ClientType_FlyKv {
					c.dir = &flykvDir{
						pdAddr: pdAddr,
					}
				}

				c.pendingSend = list.New()
				c.dir.query(c)
			}
		}
		return c, nil
	}
}

func QueryGate(pd []*net.UDPAddr, timeout time.Duration) (ret []*sproto.Flygate) {
	if resp, _ := snet.UdpCall(pd, &sproto.GetFlyGateList{}, &sproto.GetFlyGateListResp{}, timeout); nil != resp {
		ret = resp.(*sproto.GetFlyGateListResp).List
	}
	return ret
}

type dir interface {
	query(*Client) //查询可用服务
}

type flykvDir struct {
	pdAddr []*net.UDPAddr
}

func (this *flykvDir) onGates(c *Client, gates []*sproto.Flygate) (bool, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return true, 0
	}

	localGates := []string{}
	for _, c := range c.avaliableConns {
		localGates = append(localGates, c.service)
	}

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

	for _, v := range add {
		conn := &serverConn{
			service:     v.Service,
			pendingSend: list.New(),
			waitResp:    map[int64]*cmdContext{},
			c:           c,
		}
		c.avaliableConns = append(c.avaliableConns, conn)
	}

	for _, v := range remove {
		for i, conn := range c.avaliableConns {
			if conn.service == v {
				c.avaliableConns[i], c.avaliableConns[len(c.avaliableConns)-1] = c.avaliableConns[len(c.avaliableConns)-1], c.avaliableConns[i]
				c.avaliableConns = c.avaliableConns[:len(c.avaliableConns)-1]
				conn.removed = true
				if len(conn.waitResp) == 0 && nil != conn.session {
					conn.session.Close(nil, 0)
				}
				break
			}
		}
	}

	for v := c.pendingSend.Front(); v != nil; v = c.pendingSend.Front() {
		if avaliableConn := c.getAvaliable(); nil != avaliableConn {
			e := c.pendingSend.Remove(v).(*cmdContext)
			e.listElement = nil
			e.l = nil
			avaliableConn.exec(e)
		} else {
			break
		}
	}

	if len(c.avaliableConns) == 0 {
		return false, time.Millisecond * 100
	} else {
		return false, time.Second * 5
	}
}

func (this *flykvDir) query(c *Client) {
	go func() {
		closed, delay := this.onGates(c, QueryGate(this.pdAddr, time.Second))
		if closed {
			return
		} else {
			time.AfterFunc(delay, func() {
				this.query(c)
			})
		}
	}()
}
