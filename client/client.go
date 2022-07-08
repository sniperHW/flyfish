package client

import (
	"container/list"
	"errors"
	flynet "github.com/sniperHW/flyfish/pkg/net"
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

var ClientTimeout uint32 = 6000 //6sec
var maxPendingSize int = 10000
var recvTimeout time.Duration = time.Second * 30

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
	NotifyQueue        EventQueueI //响应回调的事件队列
	NotifyPriority     int         //回调事件优先级
	ClientType         ClientType
	PD                 []string
	Ordering           bool //如果需要单个client按程序顺序发送命令，设置为true
	OrderSequenceCount int64
}

var getSequenceIDTimeout error = errors.New("getSequenceIDTimeout")

type sequence struct {
	sync.Mutex
	next       int64
	max        int64
	orderCount int64
	ordering   bool
	pdAddr     []*net.UDPAddr
	stoped     int32
}

func (s *sequence) close() {
	atomic.StoreInt32(&s.stoped, 1)
}

func (s *sequence) order() {
	for atomic.LoadInt32(&s.stoped) == 0 {
		resp, _ := snet.UdpCall(s.pdAddr,
			&sproto.OrderSequenceID{
				Count: s.orderCount,
			},
			&sproto.OrderSequenceIDResp{},
			time.Second)
		if nil != resp && resp.(*sproto.OrderSequenceIDResp).Ok {
			s.Lock()
			s.ordering = false
			s.max = resp.(*sproto.OrderSequenceIDResp).Max
			s.Unlock()
			return
		} else {
			time.Sleep(time.Millisecond * 5)
		}
	}
}

func (s *sequence) get(timeout time.Duration) (seqno int64, err error) {
	var deadline time.Time
	for {
		s.Lock()
		if s.next < s.max {
			seqno = s.next
			s.next++
			if !s.ordering && s.max-s.next < s.orderCount/2 {
				s.ordering = true
				go s.order()
			}
			s.Unlock()
			return
		} else {
			s.Unlock()
			if deadline.IsZero() {
				deadline = time.Now().Add(timeout)
			} else {
				if time.Now().After(deadline) {
					err = getSequenceIDTimeout
					return
				}
			}
			runtime.Gosched()
		}
	}
	return
}

type clientImpl interface {
	exec(*cmdContext)
	close()
	start([]*net.UDPAddr)
}

type Client struct {
	impl      clientImpl
	asyncExec *asynExecMgr
	sequence  *sequence
}

type conn struct {
	service  string
	session  *flynet.Socket
	waitSend *list.List //因为连接尚未建立被排队等待发送的请求
	removed  bool
}

func (this *Client) exec(cmd *cmdContext) {
	this.asyncExec.exec(func() {
		this.impl.exec(cmd)
	})
}

func (this *Client) Close() {
	this.impl.close()
	close(this.asyncExec.stop)
	this.sequence.close()
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

	orderSequenceCount := int64(1000)

	if conf.OrderSequenceCount > 0 {
		orderSequenceCount = conf.OrderSequenceCount
	}

	c := &Client{
		asyncExec: newAsynExecMgr(conf.Ordering),
		sequence:  &sequence{next: 1, ordering: true, orderCount: orderSequenceCount, pdAddr: pdAddr},
	}
	switch conf.ClientType {
	case FlyGate:
		c.impl = &clientImplFlyGate{
			impl: impl{
				waitResp:       map[int64]*cmdContext{},
				waitSend:       list.New(),
				notifyQueue:    conf.NotifyQueue,
				notifyPriority: conf.NotifyPriority,
			},
		}
		go c.sequence.order()
		c.impl.start(pdAddr)
		return c, nil
	case FlySql:
		c.impl = &clientImplFlySql{
			impl: impl{
				waitResp:       map[int64]*cmdContext{},
				waitSend:       list.New(),
				notifyQueue:    conf.NotifyQueue,
				notifyPriority: conf.NotifyPriority,
			},
		}
		go c.sequence.order()
		c.impl.start(pdAddr)
		return c, nil
	case FlyKv:
		c.impl = &clientImplFlykv{
			waitResp:       map[int64]*cmdContext{},
			waitSend:       list.New(),
			notifyQueue:    conf.NotifyQueue,
			notifyPriority: conf.NotifyPriority,
			sets:           map[int]*set{},
			slotToStore:    map[int]*store{},
		}
		go c.sequence.order()
		c.impl.start(pdAddr)
		return c, nil
	default:
		return nil, errors.New("invaild ClientType")
	}
}
