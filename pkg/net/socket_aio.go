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
	PollerCount:     1,
	WorkerPerPoller: runtime.NumCPU(),
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

var routinePool *gopool.Pool = gopool.New(gopool.Option{
	MaxRoutineCount: 4096,
	Mode:            gopool.GoMode,
})

type SocketService struct {
	services []*goaio.AIOService
}

type ioContext struct {
	b  *buffer.Buffer
	cb func(*goaio.AIOResult, *buffer.Buffer)
}

func (this *SocketService) completeRoutine(s *goaio.AIOService) {
	for {
		res, ok := s.GetCompleteStatus()
		if !ok {
			break
		} else {
			c := res.Context.(*ioContext)
			routinePool.Go(func() {
				c.cb(&res, c.b)
			})
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
	PollerCount     int
	WorkerPerPoller int
}

func NewSocketService(o ServiceOption) *SocketService {
	s := &SocketService{}

	if o.PollerCount == 0 {
		o.PollerCount = 1
	}

	if o.WorkerPerPoller == 0 {
		o.WorkerPerPoller = 1
	}

	for i := 0; i < o.PollerCount; i++ {
		se := goaio.NewAIOService(o.WorkerPerPoller)
		s.services = append(s.services, se)
		go s.completeRoutine(se)
	}

	return s
}

type Socket struct {
	socketBase
	aioConn     *goaio.AIOConn
	sendLock    int32
	sendContext ioContext
	recvContext ioContext
	swaped      []interface{}
	task        func()
}

func (s *Socket) ShutdownWrite() {
	closeOK, remain := s.sendQueue.Close()
	if closeOK && remain == 0 && atomic.LoadInt32(&s.sendLock) == 0 {
		s.conn.(interface{ CloseWrite() error }).CloseWrite()
	}
}

func (s *Socket) onRecvComplete(r *goaio.AIOResult, _ *buffer.Buffer) {
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

		if !recvAgain || s.testFlag(fclosed|frclosed) || nil != s.aioConn.Recv1(&s.recvContext, s.inboundProcessor.GetRecvBuff(), s.getRecvTimeout()) {
			s.ioDone()
		}
	}
}

func (s *Socket) doSend(b *buffer.Buffer) {

	if nil == b {
		b = buffer.Get()
		s.sendContext.b = b
	}

	_, s.swaped = s.sendQueue.Get(s.swaped)

	for i := 0; i < len(s.swaped); i++ {
		l := b.Len()
		if err := s.encoder.EnCode(s.swaped[i], b); nil != err {
			//EnCode错误，这个包已经写入到b中的内容需要直接丢弃
			b.SetLen(l)
			GetSugar().Errorf("encode error:%v", err)

		}
		s.swaped[i] = nil
	}

	if b.Len() == 0 {
		s.onSendComplete(&goaio.AIOResult{}, b)
	} else if nil != s.aioConn.Send1(&s.sendContext, b.Bytes(), s.getSendTimeout()) {
		s.onSendComplete(&goaio.AIOResult{Err: ErrSocketClose}, b)
	}

}

func (s *Socket) Send(o interface{}) error {
	if s.encoder == nil {
		return errors.New("encoder is nil")
	} else if nil == o {
		return errors.New("o is nil")
	} else {

		if err := s.sendQueue.Add(o); nil != err {
			if err == ErrQueueClosed {
				err = ErrSocketClose
			} else if err == ErrQueueFull {
				err = ErrSendQueFull
			}
			return err
		}

		//send:2
		if atomic.CompareAndSwapInt32(&s.sendLock, 0, 1) {
			//send:3
			s.addIO()
			routinePool.Go(s.task)
		}

		return nil
	}

}

func (s *Socket) onSendComplete(r *goaio.AIOResult, b *buffer.Buffer) {
	if nil == r.Err {
		if s.sendQueue.Empty() {
			//onSendComplete:1
			atomic.StoreInt32(&s.sendLock, 0)
			//onSendComplete:2
			if !s.sendQueue.Empty() && atomic.CompareAndSwapInt32(&s.sendLock, 0, 1) {
				/*
				 * 如果a routine执行到onSendComplete:1处暂停
				 * 此时b routine执行到send:2
				 *
				 * 现在有两种情况
				 *
				 * 情况1
				 * b routine先执行完后面的代码，此时sendLock==1,因此 b不会执行send:3里面的代码
				 * a 恢复执行，发现!s.sendQueue.Empty() && atomic.CompareAndSwapInt32(&s.sendLock, 0, 1) == true
				 * 由a继续触发sendRoutinePool.GoTask(s)
				 *
				 * 情况2
				 *
				 * a routine执行到onSendComplete:2暂停
				 * b routine继续执行，此时sendLock==0，b执行send:3里面的代码
				 * a 恢复执行，发现!s.sendQueue.Empty()但是,atomic.CompareAndSwapInt32(&s.sendLock, 0, 1)失败,执行onSendComplete:3
				 *
				 */
				b.Reset()
				s.doSend(b)
				return
			} else {
				//onSendComplete:3
				if s.sendQueue.Closed() {
					s.conn.(interface{ CloseWrite() error }).CloseWrite()
					close(s.sendCloseChan)
				}
			}
		} else {
			b.Reset()
			s.doSend(b)
			return
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
			} else {
				s.errorCallback(s, r.Err)
				//如果是发送超时且用户没有关闭socket,再次请求发送
				if !s.testFlag(fclosed) {
					//超时可能会发送部分数据
					b.DropFirstNBytes(r.Bytestransfer)
					s.doSend(b)
					return
				} else {
					close(s.sendCloseChan)
				}
			}
		} else {
			close(s.sendCloseChan)
			s.Close(r.Err, 0)
		}
	}

	b.Free()

	s.ioDone()
}

func (s *Socket) BeginRecv(cb func(*Socket, interface{})) (err error) {
	s.beginOnce.Do(func() {
		if nil == cb {
			err = errors.New("BeginRecv cb is nil")
		} else if nil == s.inboundProcessor {
			err = errors.New("inboundProcessor is nil")
		} else if s.testFlag(fclosed | frclosed) {
			err = ErrSocketClose
		} else {
			//发起第一个recv
			s.inboundCallBack = cb
			s.addIO()
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
			s.aioConn.Close(nil)

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
		s.ShutdownRead()
		_, remain := s.sendQueue.Close()
		if remain > 0 && delay > 0 {
			ticker := time.NewTicker(delay)
			go func() {
				select {
				case <-s.sendCloseChan:
				case <-ticker.C:
				}

				ticker.Stop()
				s.conn.(interface{ CloseWrite() error }).CloseWrite()
			}()
		} else {
			s.conn.(interface{ CloseWrite() error }).CloseWrite()
		}

		s.closeReason = reason
		s.setFlag(fdoclose)

		if 0 == atomic.LoadInt32(&s.ioCount) {
			s.doCloseOnce.Do(func() {
				s.aioConn.Close(nil)

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
			sendQueue:     NewSendQueue(256),
		},
	}

	c, err := service.createAIOConn(conn)
	if err != nil {
		return nil
	}

	s.aioConn = c

	s.sendContext.cb = s.onSendComplete

	s.recvContext.cb = s.onRecvComplete

	s.task = func() {
		s.doSend(nil)
	}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}

func CreateSocket(conn net.Conn) *Socket {
	return NewSocket(aioService, conn)
}
