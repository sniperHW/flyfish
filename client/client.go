package client

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ClientTimeout uint32 = 6000 //6sec
var maxPendingSize int = 10000

var seqno int64

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
	Dir []string //dir服务地址
	//solo模式
	UnikeyPlacement func(string) int //返回unikey所在的store,对于连接proxy的方式无需提供,store字段由proxy填写
	SoloService     string
}

type Client struct {
	sync.Mutex
	conf        ClientConf
	index       int
	session     *flynet.Socket
	pendingSend *list.List //等待发送的消息
	waitResp    map[int64]*cmdContext
	connecting  bool
	closed      int32
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

func queryGates(dir []string) (gates []string, err error) {
	okCh := make(chan []string)
	uu := make([]*flynet.Udp, len(dir))
	for k, v := range dir {
		go func(i int, s string) {
			u, err := flynet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
			if nil == err {
				addr, err := net.ResolveUDPAddr("udp", s)
				if nil == err {
					u.SendTo(addr, &sproto.QueryGateList{})
					uu[i] = u
					recvbuff := make([]byte, 4096)
					_, r, err := u.ReadFrom(recvbuff)
					if nil == err {
						if resp, ok := r.(*sproto.GateList); ok {
							okCh <- resp.List
						}
					}
				} else {
					GetSugar().Infof("%v", err)
				}
			} else {
				GetSugar().Infof("%v", err)
			}
		}(k, v)
	}

	ticker := time.NewTicker(1 * time.Second)

	select {
	case v := <-okCh:
		gates = v
	case <-ticker.C:
		err = errors.New("timeout")

	}
	ticker.Stop()

	for _, v := range uu {
		if nil != v {
			v.Close()
		}
	}

	return
}

func (this *Client) onDisconnected() {
	this.Lock()
	this.session = nil
	waitResp := this.waitResp
	this.waitResp = map[int64]*cmdContext{}
	this.Unlock()

	for _, v := range waitResp {
		if v.deadlineTimer.Stop() {
			this.doCallBack(v.unikey, v.cb, errcode.New(errcode.Errcode_error, "lose connection"))
			releaseCmdContext(v)
		}
	}
}

func (this *Client) onConnected(session *flynet.Socket) {

	this.Lock()
	defer this.Unlock()

	this.connecting = false
	this.session = session
	this.session.SetSendQueueSize(maxPendingSize)
	this.session.SetInBoundProcessor(cs.NewRespInboundProcessor())
	this.session.SetEncoder(&cs.ReqEncoder{})
	this.session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
		GetSugar().Infof("socket close %v", reason)
		this.onDisconnected()
	}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
		this.onMessage(msg.(*cs.RespMessage))
	})

	pendingSend := this.pendingSend
	this.pendingSend = list.New()

	now := time.Now()
	//发送被排队的请求
	for v := pendingSend.Front(); v != nil; v = pendingSend.Front() {
		e := pendingSend.Remove(v).(*cmdContext)
		if remain := e.deadline.Sub(now) / time.Millisecond; remain > 0 {
			e.req.Timeout = uint32(remain)
			this.sendReq(e)
		}
	}
}

func (this *Client) connectCluster() bool {
	gates, err := queryGates(this.conf.Dir)

	if nil != err {
		return false
	}

	if len(gates) == 0 {
		return false
	}

	GetSugar().Infof("got gates%v", gates)

	this.index = rand.Int() % len(gates)

	for i := 0; i < len(gates); i++ {
		session, err := cs.NewConnector("tcp", gates[this.index]).Dial(time.Second * 5)
		if nil == err {
			this.onConnected(session)
			return true
		} else {
			this.index = (this.index + 1) % len(gates)
		}
	}

	return false
}

func (this *Client) connectSolo() bool {
	session, err := cs.NewConnector("tcp", this.conf.SoloService).Dial(time.Second * 5)
	if nil != err {
		return false
	} else {
		this.onConnected(session)
		return true
	}
}

func (this *Client) connect() {
	if !this.connecting {
		this.connecting = true
		go func() {
			ok := false
			for {
				if this.conf.SoloService == "" {
					ok = this.connectCluster()
				} else {
					ok = this.connectSolo()
				}

				if ok {
					return
				} else {
					time.Sleep(100 * time.Millisecond)
					this.Lock()
					if atomic.LoadInt32(&this.closed) == 1 || this.pendingSend.Len() == 0 {
						this.connecting = false
						this.Unlock()
						return
					} else {
						this.Unlock()
					}
				}
			}
		}()
	}
}

func (this *Client) sendReq(c *cmdContext) {
	if nil != this.conf.UnikeyPlacement {
		//如果提供了定位器，使用定位器直接计算出Store
		c.req.Store = this.conf.UnikeyPlacement(c.req.UniKey)
	}
	this.session.Send(c.req)
}

func (this *Client) exec(c *cmdContext) {
	var errCode errcode.Error
	this.Lock()
	defer func() {
		this.Unlock()
		if errCode != nil {
			this.doCallBack(c.unikey, c.cb, errCode)
			releaseCmdContext(c)
		}
	}()

	if atomic.LoadInt32(&this.closed) == 1 {
		errCode = errcode.New(errcode.Errcode_error, "client closed")
	} else {
		c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
		if nil != this.session {
			this.waitResp[c.req.Seqno] = c
			c.req.Timeout = ClientTimeout
			c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
			this.sendReq(c)
		} else {
			this.connect()
			if this.pendingSend.Len() < maxPendingSize {
				this.waitResp[c.req.Seqno] = c
				c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
				c.listElement = this.pendingSend.PushBack(c)
				c.l = this.pendingSend
			} else {
				errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
			}
		}
	}
}

func (this *Client) Close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.Lock()
		session := this.session
		this.Unlock()
		if nil != session {
			session.Close(nil, 0)
		}
	}
}

func OpenClient(conf ClientConf) (*Client, error) {
	c := &Client{
		conf:        conf,
		pendingSend: list.New(),
		waitResp:    map[int64]*cmdContext{},
	}

	if "" == conf.SoloService && len(conf.Dir) == 0 {
		return nil, errors.New("cluster mode,but dir empty")
	} else {
		return c, nil
	}
}
