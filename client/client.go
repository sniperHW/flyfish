package client

import (
	"container/list"
	"errors"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ClientType int

const resendDelay time.Duration = time.Millisecond * 100

var ClientTimeout time.Duration = time.Second * 5 //6sec
var maxPendingSize int64 = 10000
var recvTimeout time.Duration = time.Second * 30
var SequenceOrderStep int64 = 1000

const (
	FlyKv   = ClientType(1) //请求发往flykv
	FlySql  = ClientType(2) //请求发往flysql
	FlyGate = ClientType(3) //请求发往flygate由flygate负责转发
)

type EventQueueI interface {
	Post(priority int, fn interface{}, args ...interface{}) error
}

var outputBufLimit flynet.OutputBufLimit = flynet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

type ClientConf struct {
	NotifyQueue    EventQueueI //响应回调的事件队列
	NotifyPriority int         //回调事件优先级
	ClientType     ClientType
	PD             []string
	Ordering       bool //如果需要单个client按程序顺序发送命令，设置为true
}

var getSequenceIDTimeout error = errors.New("getSequenceIDTimeout")

type idbuffer struct {
	next int64
	max  int64
}

func (b *idbuffer) nextID() (id int64, free int64) {
	id = b.next
	b.next++
	free = b.max - b.next
	return
}

type sequence struct {
	sync.Mutex
	buffer   []*idbuffer
	step     int64
	ordering bool
	pdAddr   []*net.UDPAddr
	stoped   int32
}

func NewSequence(pdAddr []*net.UDPAddr, step int64) *sequence {
	return &sequence{
		pdAddr: pdAddr,
		step:   step,
	}
}

func (s *sequence) Close() {
	atomic.StoreInt32(&s.stoped, 1)
}

func (s *sequence) order() {
	for atomic.LoadInt32(&s.stoped) == 0 {
		resp, _ := snet.UdpCall(s.pdAddr,
			&sproto.OrderSequenceID{
				Count: s.step,
			},
			&sproto.OrderSequenceIDResp{},
			time.Second)
		if nil != resp && resp.(*sproto.OrderSequenceIDResp).Ok {
			s.Lock()
			buffer := &idbuffer{
				max:  resp.(*sproto.OrderSequenceIDResp).Max,
				next: resp.(*sproto.OrderSequenceIDResp).Max - s.step,
			}
			if buffer.next == 0 {
				buffer.next = 1
			}
			s.buffer = append(s.buffer, buffer)
			s.ordering = false
			s.Unlock()
			return
		} else {
			time.Sleep(time.Millisecond * 5)
		}
	}
}

func (s *sequence) next() int64 {
	s.Lock()
	defer s.Unlock()
	var id, freeCount int64
	if len(s.buffer) > 0 {
		if id, freeCount = s.buffer[0].nextID(); freeCount == 0 {
			//当前buffer已经用光，切换到后续buffer(如果有)
			for i := 1; i < len(s.buffer); i++ {
				s.buffer[i-1] = s.buffer[i]
			}
			s.buffer[len(s.buffer)-1] = nil
			s.buffer = s.buffer[:len(s.buffer)-1]
		}
	}
	if !s.ordering && len(s.buffer) < 2 {
		s.ordering = true
		go s.order()
	}
	return id
}

func (s *sequence) Next(timeout time.Duration) (seqno int64, err error) {
	var deadline time.Time
	for {
		if id := s.next(); id > 0 {
			seqno = id
			break
		} else {
			if deadline.IsZero() {
				deadline = time.Now().Add(timeout)
			} else {
				if time.Now().After(deadline) {
					err = getSequenceIDTimeout
					break
				}
			}
			runtime.Gosched()
		}
	}
	return
}

type conn struct {
	service  string
	session  *flynet.Socket
	waitSend *list.List //因为连接尚未建立被排队等待发送的请求
	removed  bool
}

type clientImpl interface {
	exec(*cmdContext)
	close()
	start([]*net.UDPAddr)
}

type clientImplBase struct {
	mu             sync.Mutex
	closed         int32
	waitResp       map[int64]*cmdContext
	waitSend       *list.List  //len(avaliableConns)==0时被排队等待发送的请求
	notifyQueue    EventQueueI //响应回调的事件队列
	notifyPriority int         //回调事件优先级
	pendingCount   int64
	asyncExec      *asynExecMgr
	sequence       *sequence
	funcSend       func(*cmdContext, time.Time)
}

func (this *clientImplBase) onTimeout(cmd *cmdContext) {
	cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_timeout, "timeout")), func() {
		atomic.AddInt64(&this.pendingCount, -1)
		this.mu.Lock()
		cmd.stopTimer()
		delete(this.waitResp, cmd.req.Seqno)
		if nil != cmd.listElement {
			cmd.l.Remove(cmd.listElement)
		}
		this.mu.Unlock()
	})
}

func (this *clientImplBase) execError(cmd *cmdContext, errCode errcode.Error) {
	cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errCode), func() {
		atomic.AddInt64(&this.pendingCount, -1)
	})
}

func (this *clientImplBase) exec(cmd *cmdContext) {
	pendingCount := atomic.AddInt64(&this.pendingCount, 1)
	if atomic.LoadInt32(&this.closed) == 1 {
		go this.execError(cmd, errcode.New(errcode.Errcode_error, "client closed"))
	} else if pendingCount > maxPendingSize {
		go this.execError(cmd, errcode.New(errcode.Errcode_retry, "busy please retry later"))
	} else {
		this.asyncExec.exec(func() {
			if timeout := cmd.deadline.Sub(time.Now()); timeout > 0 {
				seqno, err := this.sequence.Next(timeout)
				if nil != err {
					this.execError(cmd, errcode.New(errcode.Errcode_error, "get seqno failed"))
				} else {
					now := time.Now()
					if timeout = cmd.deadline.Sub(now); timeout > 0 {
						this.mu.Lock()
						cmd.req.Seqno = seqno
						this.waitResp[cmd.req.Seqno] = cmd
						cmd.deadlineTimer = time.AfterFunc(timeout, func() { this.onTimeout(cmd) })
						this.funcSend(cmd, now)
						this.mu.Unlock()
					} else {
						this.execError(cmd, errcode.New(errcode.Errcode_timeout))
					}
				}
			} else {
				this.execError(cmd, errcode.New(errcode.Errcode_timeout))
			}
		})
	}
}

func (this *clientImplBase) sendAgain(cmd *cmdContext) {
	this.mu.Lock()
	if nil != this.waitResp[cmd.req.Seqno] {
		if atomic.LoadInt32(&this.closed) == 1 {
			delete(this.waitResp, cmd.req.Seqno)
			cmd.stopTimer()
			this.mu.Unlock()
			cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_error, "client closed")), func() {
				atomic.AddInt64(&this.pendingCount, -1)
			})
		} else {
			this.funcSend(cmd, time.Now())
			this.mu.Unlock()
		}
	} else {
		this.mu.Unlock()
	}
}

func (this *clientImplBase) onResponse(msg *cs.RespMessage, ctx *cmdContext) {
	var ret interface{}
	switch protocol.CmdType(msg.Cmd) {
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
	ctx.doCallBack(this.notifyQueue, this.notifyPriority, ret, func() {
		atomic.AddInt64(&this.pendingCount, -1)
	})
}

func (this *clientImplBase) close() {
	this.sequence.Close()
	this.asyncExec.close()
}

type Client struct {
	impl clientImpl
}

func (this *Client) exec(cmd *cmdContext) {
	this.impl.exec(cmd)
}

func (this *Client) Close() {
	this.impl.close()
}

func New(conf ClientConf) (*Client, error) {
	if len(conf.PD) == 0 {
		return nil, errors.New("PD is empty")
	}

	var pdAddr []*net.UDPAddr

	for _, v := range conf.PD {
		if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
			pdAddr = append(pdAddr, addr)
		} else {
			return nil, err
		}
	}

	c := &Client{}
	asyncExec := newAsynExecMgr(conf.Ordering)
	sequence := NewSequence(pdAddr, SequenceOrderStep)

	base := clientImplBase{
		waitResp:       map[int64]*cmdContext{},
		waitSend:       list.New(),
		notifyQueue:    conf.NotifyQueue,
		notifyPriority: conf.NotifyPriority,
		asyncExec:      asyncExec,
		sequence:       sequence,
	}

	switch conf.ClientType {
	case FlyGate:
		impl := &clientImplFlyGate{
			impl: impl{
				clientImplBase: base,
			},
		}
		impl.funcSend = impl.send
		c.impl = impl
	case FlySql:
		impl := &clientImplFlySql{
			impl: impl{
				clientImplBase: base,
			},
		}
		impl.funcSend = impl.send
		c.impl = impl
	case FlyKv:
		impl := &clientImplFlykv{
			sets:           map[int]*set{},
			slotToStore:    map[int]*store{},
			clientImplBase: base,
		}
		impl.funcSend = impl.send
		c.impl = impl
	default:
		return nil, errors.New("invaild ClientType")
	}

	c.impl.start(pdAddr)
	return c, nil
}
