// +build darwin netbsd freebsd openbsd dragonfly linux

package aio

import (
	"container/list"
	//"fmt"
	"github.com/sniperHW/aiogo"
	"github.com/sniperHW/kendynet"
	//"github.com/sniperHW/kendynet/util"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	fstarted = int32(1 << 0)
	fclosed  = int32(1 << 1)
	fwclosed = int32(1 << 2)
	frclosed = int32(1 << 3)
)

type AioReceiver interface {
	ReceiveAndUnpack(kendynet.StreamSession) (interface{}, error)
	OnRecvOk(kendynet.StreamSession, []byte)
	StartReceive(kendynet.StreamSession)
	OnClose()
}

type defaultReceiver struct {
	bytes  int
	buffer []byte
}

func (this *defaultReceiver) StartReceive(s kendynet.StreamSession) {
	s.(*AioSocket).Recv(this.buffer)
}

func (this *defaultReceiver) ReceiveAndUnpack(s kendynet.StreamSession) (interface{}, error) {
	for {
		if 0 != this.bytes {
			msg := kendynet.NewByteBuffer(this.bytes)
			msg.AppendBytes(this.buffer[:this.bytes])
			this.bytes = 0
			return msg, nil
		} else {
			return nil, s.(*AioSocket).Recv(this.buffer)
		}
	}
}

func (this *defaultReceiver) OnRecvOk(_ kendynet.StreamSession, buff []byte) {
	this.bytes = len(buff)
}

func (this *defaultReceiver) OnClose() {

}

type AioSocket struct {
	muW              sync.Mutex
	ud               atomic.Value
	receiver         AioReceiver
	flag             int32
	onClose          atomic.Value
	encoder          atomic.Value
	onEvent          func(*kendynet.Event)
	aioConn          *aiogo.Conn
	sendBuffs        [][]byte
	pendingSend      *list.List
	watcher          *aiogo.Watcher
	sendLock         bool
	rcompleteQueue   *aiogo.CompleteQueue
	wcompleteQueue   *aiogo.CompleteQueue
	sendQueueSize    int
	onClearSendQueue atomic.Value
	closeReason      string
	maxPostSendSize  int
	CBLock           sync.Mutex
	closeOnce        sync.Once
	startOnce        sync.Once
	sendCount        int32
	recvCount        int32
}

func NewAioSocket(service *AioService, netConn net.Conn) kendynet.StreamSession {

	w, rq, wq := service.getWatcherAndCompleteQueue()

	c, err := w.Watch(netConn)
	if err != nil {
		return nil
	}

	s := &AioSocket{
		aioConn:         c,
		watcher:         w,
		rcompleteQueue:  rq,
		wcompleteQueue:  wq,
		sendQueueSize:   256,
		sendBuffs:       make([][]byte, 512),
		pendingSend:     list.New(),
		maxPostSendSize: 1024 * 1024,
	}

	runtime.SetFinalizer(s, func(s *AioSocket) {
		s.Close("gc", 0)
	})

	return s
}

func emptyFunc(kendynet.StreamSession, string) {

}

func (this *AioSocket) clearup() {
	if nil != this.receiver {
		this.receiver.OnClose()
	}

	if onClose := this.onClose.Load(); nil != onClose {
		onClose.(func(kendynet.StreamSession, string))(this, this.closeReason)
		//this.onClose.Store(emptyFunc)
	}
	//this.onEvent = nil
}

//保证onEvent在读写线程中按序执行
func (this *AioSocket) callEventCB(event *kendynet.Event, oflag int32) {
	for {
		flag := atomic.LoadInt32(&this.flag)
		if flag&(fclosed|fwclosed) > 0 {
			return
		} else if atomic.CompareAndSwapInt32(&this.flag, flag, flag|oflag) {
			break
		}
	}
	/*
	 *  这个锁在绝大多数情况下无竞争，只有在sendThreadFunc发生错误需要调用onEvent时才可能发生竞争
	 */
	this.CBLock.Lock()
	this.onEvent(event)
	this.CBLock.Unlock()
}

func (this *AioSocket) setFlag(flag int32) {
	for !atomic.CompareAndSwapInt32(&this.flag, this.flag, this.flag|flag) {
	}
}

func (this *AioSocket) testFlag(flag int32) bool {
	return atomic.LoadInt32(&this.flag)&flag > 0
}

func (this *AioSocket) onRecvComplete(r *aiogo.CompleteEvent) {

	defer func() {
		if atomic.AddInt32(&this.recvCount, -1) == 0 && this.testFlag(fclosed|frclosed) && atomic.LoadInt32(&this.sendCount) == 0 {
			this.clearup()
		}
	}()

	if nil != r.Err {
		flag := int32(0)
		if r.Err == aiogo.ErrRecvTimeout {
			r.Err = kendynet.ErrRecvTimeout
		} else {
			if r.Err == aiogo.ErrEof {
				r.Err = io.EOF
			}
			this.shutdownRead()
			flag = frclosed
		}

		this.callEventCB(&kendynet.Event{
			Session:   this,
			EventType: kendynet.EventTypeError,
			Data:      r.Err,
		}, flag)

	} else {
		this.receiver.OnRecvOk(this, r.GetBuff())
		for !this.testFlag(fclosed | frclosed) {
			msg, err := this.receiver.ReceiveAndUnpack(this)
			if nil != err {
				this.shutdownRead()
				this.callEventCB(&kendynet.Event{
					Session:   this,
					EventType: kendynet.EventTypeError,
					Data:      err,
				}, frclosed)
			} else if msg != nil {
				this.callEventCB(&kendynet.Event{
					Session:   this,
					EventType: kendynet.EventTypeMessage,
					Data:      msg,
				}, int32(0))
			} else {
				break
			}
		}
	}
}

func (this *AioSocket) Recv(buff []byte) error {

	flag := atomic.LoadInt32(&this.flag)

	if flag&(fclosed|frclosed) > 0 {
		return kendynet.ErrSocketClose
	}

	if (flag & fstarted) == 0 {
		return kendynet.ErrNotStart
	}

	atomic.AddInt32(&this.recvCount, 1)

	return this.aioConn.Recv(buff, this, this.rcompleteQueue)
}

func (this *AioSocket) emitSendRequest() {
	c := 0
	totalSize := 0
	for v := this.pendingSend.Front(); v != nil; v = this.pendingSend.Front() {
		this.pendingSend.Remove(v)
		this.sendBuffs[c] = v.Value.(kendynet.Message).Bytes()
		totalSize += len(this.sendBuffs[c])
		c++
		if c >= len(this.sendBuffs) || totalSize >= this.maxPostSendSize {
			break
		}
	}
	atomic.AddInt32(&this.sendCount, 1)
	this.aioConn.SendBuffers(this.sendBuffs[:c], this, this.wcompleteQueue)
}

func (this *AioSocket) onSendComplete(r *aiogo.CompleteEvent) {

	defer func() {
		if 0 == atomic.AddInt32(&this.sendCount, -1) && this.testFlag(fclosed|fwclosed) && atomic.LoadInt32(&this.recvCount) == 0 {
			this.clearup()
		}
	}()

	if nil == r.Err {
		this.muW.Lock()
		if this.pendingSend.Len() == 0 {
			this.sendLock = false
			this.muW.Unlock()
			if onClearSendQueue := this.onClearSendQueue.Load(); nil != onClearSendQueue {
				onClearSendQueue.(func())()
			}
		} else {
			this.emitSendRequest()
			this.muW.Unlock()
		}
	} else {

		flag := int32(0)

		if r.Err == aiogo.ErrSendTimeout {
			r.Err = kendynet.ErrSendTimeout
		} else {
			if r.Err == aiogo.ErrEof {
				r.Err = io.EOF
			}
			flag = fwclosed
		}

		this.callEventCB(&kendynet.Event{
			Session:   this,
			EventType: kendynet.EventTypeError,
			Data:      r.Err,
		}, flag)
	}
}

func (this *AioSocket) Send(o interface{}) error {
	if o == nil {
		return kendynet.ErrInvaildObject
	}

	var encoder kendynet.EnCoder
	if v := this.encoder.Load(); nil != v {
		encoder = v.(kendynet.EnCoder)
	} else {
		return kendynet.ErrInvaildEncoder
	}

	msg, err := encoder.EnCode(o)

	if err != nil {
		return err
	}

	return this.sendMessage(msg)
}

func (this *AioSocket) sendMessage(msg kendynet.Message) error {

	if this.testFlag(fclosed | fwclosed) {
		return kendynet.ErrSocketClose
	}

	this.muW.Lock()
	defer this.muW.Unlock()

	if this.pendingSend.Len() > this.sendQueueSize {
		return kendynet.ErrSendQueFull
	}

	this.pendingSend.PushBack(msg)

	if !this.sendLock && this.testFlag(fstarted) {
		this.sendLock = true
		this.emitSendRequest()
	}
	return nil
}

func (this *AioSocket) SendMessage(msg kendynet.Message) error {
	if msg == nil {
		return kendynet.ErrInvaildBuff
	}

	return this.sendMessage(msg)
}

func (this *AioSocket) shutdownRead() {
	this.aioConn.GetRowConn().(interface{ CloseRead() error }).CloseRead()
}

func (this *AioSocket) Close(reason string, delay time.Duration) {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)

		this.setFlag(fclosed)

		this.closeReason = reason

		if this.testFlag(fwclosed) {
			delay = 0 //写端已经关闭，delay参数没有意义设置为0
		}

		this.muW.Lock()
		if this.pendingSend.Len() > 0 {
			delay = delay * time.Second
			if delay <= 0 {
				this.pendingSend = list.New()
			}
		}
		this.muW.Unlock()

		if delay > 0 {
			ch := make(chan struct{})
			this.onClearSendQueue.Store(func() {
				close(ch)
			})

			this.muW.Lock()
			if !this.sendLock {
				this.sendLock = true
				this.emitSendRequest()
			}
			this.muW.Unlock()

			this.shutdownRead()
			ticker := time.NewTicker(delay)
			go func() {
				select {
				case <-ch:
				case <-ticker.C:
				}

				ticker.Stop()
				this.aioConn.Close()
			}()
		} else {
			this.aioConn.Close()
			if atomic.LoadInt32(&this.recvCount) == 0 && atomic.LoadInt32(&this.sendCount) == 0 {
				this.clearup()
			}
		}
	})
}

func (this *AioSocket) IsClosed() bool {
	return this.testFlag(fclosed)
}

func (this *AioSocket) SetCloseCallBack(cb func(kendynet.StreamSession, string)) {
	this.onClose.Store(cb)
}

func (this *AioSocket) SetReceiver(r kendynet.Receiver) {
	if aio_r, ok := r.(AioReceiver); ok {
		this.receiver = aio_r
	}
}

func (this *AioSocket) SetEncoder(encoder kendynet.EnCoder) {
	this.encoder.Store(encoder)
}

func (this *AioSocket) Start(eventCB func(*kendynet.Event)) error {
	err := kendynet.ErrStarted

	this.startOnce.Do(func() {
		for {

			flag := atomic.LoadInt32(&this.flag)

			if flag&fclosed > 0 {
				err = kendynet.ErrSocketClose
				return
			} else if atomic.CompareAndSwapInt32(&this.flag, flag, flag|fstarted) {
				break
			}
		}

		if this.receiver == nil {
			this.receiver = &defaultReceiver{buffer: make([]byte, 4096)}
		}

		this.onEvent = eventCB

		//发起第一个recv
		this.receiver.StartReceive(this)

		//如果有待发送数据，启动send
		this.muW.Lock()
		if this.pendingSend.Len() > 0 {
			this.sendLock = true
			this.emitSendRequest()
		}
		this.muW.Unlock()

		err = nil

	})

	return err
}

func (this *AioSocket) LocalAddr() net.Addr {
	return this.aioConn.GetRowConn().LocalAddr()
}

func (this *AioSocket) RemoteAddr() net.Addr {
	return this.aioConn.GetRowConn().RemoteAddr()
}

func (this *AioSocket) SetUserData(ud interface{}) {
	this.ud.Store(ud)
}

func (this *AioSocket) GetUserData() interface{} {
	return this.ud.Load()
}

func (this *AioSocket) GetUnderConn() interface{} {
	return this.aioConn.GetRowConn()
}

func (this *AioSocket) SetRecvTimeout(timeout time.Duration) {
	this.aioConn.SetRecvTimeout(timeout)
}

func (this *AioSocket) SetSendTimeout(timeout time.Duration) {
	this.aioConn.SetSendTimeout(timeout)
}

func (this *AioSocket) SetMaxPostSendSize(size int) {
	this.muW.Lock()
	defer this.muW.Unlock()
	this.maxPostSendSize = size
}

func (this *AioSocket) SetSendQueueSize(size int) {
	this.muW.Lock()
	defer this.muW.Unlock()
	this.sendQueueSize = size
}
