// +build aio

package net

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/gopool"
	"github.com/sniperHW/goaio"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type bufferPool struct {
	pool sync.Pool
}

const PoolBuffSize uint64 = 1024 * 1024 * 2

var buffPool *bufferPool = newBufferPool()

var aioService *SocketService = NewSocketService(ServiceOption{
	PollerCount:              1,
	WorkerPerPoller:          runtime.NumCPU(),
	CompleteRoutinePerPoller: 4,
})

func newBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, PoolBuffSize)
			},
		},
	}
}

func (p *bufferPool) Acquire() []byte {
	return p.pool.Get().([]byte)
}

func (p *bufferPool) Release(buff []byte) {
	if uint64(cap(buff)) == PoolBuffSize {
		p.pool.Put(buff[:cap(buff)])
	}
}

func GetBuffPool() *bufferPool {
	return buffPool
}

var sendRoutinePool *gopool.Pool = gopool.New(gopool.Option{
	MaxRoutineCount: 1024,
	Mode:            gopool.QueueMode,
})

type SocketService struct {
	services []*goaio.AIOService
}

type ioContext struct {
	s *Socket
	t rune
}

func (this *SocketService) completeRoutine(s *goaio.AIOService) {
	for {
		res, ok := s.GetCompleteStatus()
		if !ok {
			break
		} else {
			context := res.Context.(*ioContext)
			if context.t == 'r' {
				context.s.onRecvComplete(&res)
			} else {
				context.s.onSendComplete(&res)
			}
		}
	}
}

func (this *SocketService) createAIOConn(conn net.Conn) (*goaio.AIOConn, error) {
	idx := rand.Int() % len(this.services)
	c, err := this.services[idx].CreateAIOConn(conn, goaio.AIOConnOption{
		ShareBuff: GetBuffPool(),
	})
	return c, err
}

func (this *SocketService) Close() {
	for i, _ := range this.services {
		this.services[i].Close()
	}
}

type ServiceOption struct {
	PollerCount              int
	WorkerPerPoller          int
	CompleteRoutinePerPoller int
}

func NewSocketService(o ServiceOption) *SocketService {
	s := &SocketService{}

	if o.PollerCount == 0 {
		o.PollerCount = 1
	}

	if o.WorkerPerPoller == 0 {
		o.WorkerPerPoller = 1
	}

	if o.CompleteRoutinePerPoller == 0 {
		o.CompleteRoutinePerPoller = 1
	}

	for i := 0; i < o.PollerCount; i++ {
		se := goaio.NewAIOService(o.WorkerPerPoller)
		s.services = append(s.services, se)
		for j := 0; j < o.CompleteRoutinePerPoller; j++ {
			go s.completeRoutine(se)
		}
	}

	return s
}

var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &item{}
	},
}

type item struct {
	nnext *item
	v     interface{}
}

type sendqueue struct {
	head *item
	tail *item
	cap  int
	len  int
}

func newSendQueue(cap int) sendqueue {
	return sendqueue{
		cap: cap,
	}
}

func (r *sendqueue) empty() bool {
	return r.len == 0
}

func (r *sendqueue) setCap(cap int) {
	r.cap = cap
}

func (r *sendqueue) pop() interface{} {
	if r.len == 0 {
		return nil
	} else {
		f := r.head
		v := f.v
		r.head = f.nnext
		if r.head == nil {
			r.tail = nil
		}
		f.nnext = nil
		f.v = nil
		itemPool.Put(f)
		r.len--
		return v
	}
}

func (r *sendqueue) push(v interface{}) bool {
	if r.len == r.cap {
		return false
	} else {
		it := itemPool.Get().(*item)
		it.v = v
		if nil == r.tail {
			r.head = it
		} else {
			r.tail.nnext = it
		}
		r.tail = it
		r.len++
		return true
	}
}

type Socket struct {
	socketBase
	muW         sync.Mutex
	sendQueue   sendqueue
	aioConn     *goaio.AIOConn
	sendLock    bool
	sendContext ioContext
	recvContext ioContext
	b           *buffer.Buffer
}

func (s *Socket) ShutdownWrite() {
	if s.testFlag(fclosed | fwclosed) {
		return
	} else {
		s.setFlag(fwclosed)
		s.muW.Lock()
		defer s.muW.Unlock()
		if s.sendQueue.empty() && !s.sendLock {
			s.conn.(interface{ CloseWrite() error }).CloseWrite()
			close(s.sendCloseChan)
		}
	}
}

func (s *Socket) SetSendQueueSize(size int) *Socket {
	s.muW.Lock()
	defer s.muW.Unlock()
	s.sendQueue.setCap(size)
	return s
}

func (s *Socket) onRecvComplete(r *goaio.AIOResult) {
	if s.testFlag(fclosed | frclosed) {
		s.ioDone()
	} else {
		recvAgain := false

		if nil != r.Err {

			if r.Err == goaio.ErrRecvTimeout {
				r.Err = ErrRecvTimeout
				recvAgain = true
			} else {
				s.setFlag(frclosed)
			}

			if nil != s.errorCallback {
				s.errorCallback(s, r.Err)
			} else {
				s.Close(r.Err, 0)
			}

		} else {
			s.inboundProcessor.OnData(r.Buff[:r.Bytestransfer])
			for !s.testFlag(fclosed | frclosed) {
				msg, err := s.inboundProcessor.Unpack()
				if nil != err {
					s.Close(err, 0)
					if nil != s.errorCallback {
						s.errorCallback(s, err)
					}
					break
				} else if nil != msg {
					s.inboundCallBack(s, msg)
				} else {
					recvAgain = true
					break
				}
			}
		}

		if !recvAgain || s.testFlag(fclosed|frclosed) || nil != s.aioConn.Recv(&s.recvContext, s.inboundProcessor.GetRecvBuff(), s.getRecvTimeout()) {
			s.ioDone()
		}
	}
}

func (s *Socket) prepareSendBuff() {

	//只有之前请求的buff全部发送完毕才填充新的buff
	if nil == s.b {
		s.b = buffer.Get()
	}

	for v := s.sendQueue.pop(); nil != v; v = s.sendQueue.pop() {
		l := s.b.Len()
		if err := s.encoder.EnCode(v, s.b); nil != err {
			//EnCode错误，这个包已经写入到b中的内容需要直接丢弃
			s.b.SetLen(l)
			GetSugar().Errorf("encode error:%v", err)

		}
	}
}

/*
 *  实现gopool.Task接口,避免无谓的闭包创建
 */

func (s *Socket) Do() {
	s.doSend()
}

func (s *Socket) doSend() {

	s.muW.Lock()
	s.prepareSendBuff()
	s.muW.Unlock()

	if s.b.Len() == 0 {
		s.onSendComplete(&goaio.AIOResult{})
	} else if nil != s.aioConn.Send(&s.sendContext, s.b.Bytes(), s.getSendTimeout()) {
		s.onSendComplete(&goaio.AIOResult{Err: ErrSocketClose})
	}

}

func (s *Socket) releaseb() {
	if nil != s.b {
		s.b.Free()
		s.b = nil
	}
}

func (s *Socket) onSendComplete(r *goaio.AIOResult) {
	defer s.ioDone()
	if nil == r.Err {
		s.muW.Lock()
		//发送完成释放发送buff
		if s.sendQueue.empty() {
			s.releaseb()
			s.sendLock = false
			if s.testFlag(fwclosed) {
				s.conn.(interface{ CloseWrite() error }).CloseWrite()
				close(s.sendCloseChan)
			}
			s.muW.Unlock()
		} else {
			s.muW.Unlock()
			s.b.Reset()
			s.addIO()
			sendRoutinePool.GoTask(s)
		}
	} else if !s.testFlag(fclosed) {
		if r.Err == goaio.ErrSendTimeout {
			r.Err = ErrSendTimeout
		}

		if nil != s.errorCallback {
			if r.Err != ErrSendTimeout {
				close(s.sendCloseChan)
				s.Close(r.Err, 0)
				s.errorCallback(s, r.Err)
				s.releaseb()
			} else {
				s.errorCallback(s, r.Err)
				if !s.testFlag(fclosed) {
					//超时可能会发送部分数据
					s.b.DropFirstNBytes(r.Bytestransfer)
					s.addIO()
					sendRoutinePool.GoTask(s)
				} else {
					close(s.sendCloseChan)
					s.releaseb()
				}
			}
		} else {
			close(s.sendCloseChan)
			s.Close(r.Err, 0)
			s.releaseb()
		}
	} else {
		s.releaseb()
	}
}

func (s *Socket) Send(o interface{}) error {
	if s.encoder == nil {
		return errors.New("encoder is nil")
	} else if nil == o {
		return errors.New("o is nil")
	} else {

		if s.testFlag(fclosed|fwclosed) > 0 {
			return ErrSocketClose
		}

		s.muW.Lock()
		if !s.sendQueue.push(o) {
			s.muW.Unlock()
			return ErrSendQueFull
		}

		if !s.sendLock {
			s.addIO()
			s.sendLock = true
			s.muW.Unlock()
			sendRoutinePool.GoTask(s)
		} else {
			s.muW.Unlock()
		}

		return nil
	}

}

func (s *Socket) BeginRecv(cb func(*Socket, interface{})) (err error) {
	s.beginOnce.Do(func() {
		if nil == cb {
			err = errors.New("BeginRecv cb is nil")
			return
		}

		if nil == s.inboundProcessor {
			err = errors.New("inboundProcessor is nil")
			return
		}

		s.addIO()
		if s.testFlag(fclosed | frclosed) {
			s.ioDone()
			err = ErrSocketClose
		} else {
			//发起第一个recv
			s.inboundCallBack = cb
			if err = s.aioConn.Recv(&s.recvContext, s.inboundProcessor.GetRecvBuff(), s.getRecvTimeout()); nil != err {
				s.ioDone()
			}
		}
	})
	return
}

func (s *Socket) ioDone() {
	if 0 == atomic.AddInt32(&s.ioCount, -1) && s.testFlag(fdoclose) {
		s.doCloseOnce.Do(func() {
			if nil != s.inboundProcessor {
				s.inboundProcessor.OnSocketClose()
			}
			if nil != s.closeCallBack {
				s.closeCallBack(s, s.closeReason)
			}
		})
	}
}

func (s *Socket) Close(reason error, delay time.Duration) {
	s.closeOnce.Do(func() {
		runtime.SetFinalizer(s, nil)
		s.setFlag(fclosed)

		if !s.testFlag(fwclosed) && delay > 0 {
			s.ShutdownRead()
			ticker := time.NewTicker(delay)
			go func() {
				select {
				case <-s.sendCloseChan:
				case <-ticker.C:
				}

				ticker.Stop()
				s.aioConn.Close(nil)
			}()
		} else {
			s.aioConn.Close(nil)
		}

		s.setFlag(fdoclose)
		s.closeReason = reason

		if 0 == atomic.LoadInt32(&s.ioCount) {
			s.doCloseOnce.Do(func() {
				if nil != s.inboundProcessor {
					s.inboundProcessor.OnSocketClose()
				}
				if nil != s.closeCallBack {
					s.closeCallBack(s, s.closeReason)
				}
			})
		}
	})
}

func NewSocket(service *SocketService, conn net.Conn) *Socket {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	s := &Socket{
		socketBase: socketBase{
			conn:          conn,
			sendCloseChan: make(chan struct{}),
		},
	}

	c, err := service.createAIOConn(conn)
	if err != nil {
		return nil
	}

	s.aioConn = c
	s.sendQueue = newSendQueue(256)
	s.sendContext = ioContext{s: s, t: 's'}
	s.recvContext = ioContext{s: s, t: 'r'}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}

func CreateSocket(conn net.Conn) *Socket {
	return NewSocket(aioService, conn)
}
