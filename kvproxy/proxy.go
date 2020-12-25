package kvproxy

import (
	"fmt"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/timer"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	minSize        uint64 = net.SizeLen
	initBufferSize uint64 = 1024 * 256
)

func isPow2(size uint64) bool {
	return (size & (size - 1)) == 0
}

func sizeofPow2(size uint64) uint64 {
	if isPow2(size) {
		return size
	}
	size = size - 1
	size = size | (size >> 1)
	size = size | (size >> 2)
	size = size | (size >> 4)
	size = size | (size >> 8)
	size = size | (size >> 16)
	return size + 1
}

type pendingReq struct {
	seqno         int64
	oriSeqno      int64
	session       kendynet.StreamSession
	deadlineTimer *timer.Timer
	processor     *reqProcessor
}

type kvproxy struct {
	router     *reqRouter
	processors []*reqProcessor
	listener   *net.Listener
	seqno      int64
	respChan   chan *kendynet.ByteBuffer
}

func (this *pendingReq) onTimeout(_ *timer.Timer, _ interface{}) {
	this.processor.Lock()
	defer this.processor.Unlock()
	logger.Infoln("remove timeout req", this.seqno)
	delete(this.processor.pendingReqs, this.seqno)
}

type reqProcessor struct {
	sync.Mutex
	pendingReqs map[int64]*pendingReq
	timerMgr    *timer.TimerMgr
	router      *reqRouter
}

func newReqProcessor(router *reqRouter) *reqProcessor {
	return &reqProcessor{
		pendingReqs: map[int64]*pendingReq{},
		timerMgr:    timer.NewTimerMgr(1),
		router:      router,
	}
}

func (this *reqProcessor) onReq(seqno int64, session kendynet.StreamSession, req *kendynet.ByteBuffer) {

	var err error
	var oriSeqno int64
	var lenUnikey int16
	var unikey string
	var timeout uint32
	var cmd uint16
	var b []byte

	if oriSeqno, err = req.GetInt64(5); nil != err {
		return
	}

	if lenUnikey, err = req.GetInt16(21); nil != err {
		return
	}

	if 0 == lenUnikey {
		return
	}

	if b, err = req.GetBytes(23, uint64(lenUnikey)); nil != err {
		return
	}

	//unikey不会在函数作用域以外被使用,unsafe强转是安全的
	unikey = *(*string)(unsafe.Pointer(&b))

	if timeout, err = req.GetUint32(17); nil != err {
		return
	}

	if cmd, err = req.GetUint16(23 + uint64(lenUnikey)); nil != err {
		return
	}

	if cmd == uint16(protocol.CmdType_Ping) {
		//返回心跳
		resp := net.NewMessage(net.CommonHead{}, &protocol.PingResp{
			Timestamp: time.Now().UnixNano(),
		})
		session.Send(resp)
		return
	}

	//用seqno替换oriSeqno
	req.PutInt64(5, seqno)

	err = func() error {
		this.Lock()
		defer this.Unlock()
		err := this.router.forward2kvnode(unikey, time.Now().Add(time.Duration(timeout/2)*time.Millisecond), req, session.GetUserData().(bool))
		if nil == err {
			pReq := &pendingReq{
				seqno:     seqno,
				oriSeqno:  oriSeqno,
				session:   session,
				processor: this,
			}
			pReq.deadlineTimer = this.timerMgr.Once(time.Duration(timeout)*time.Millisecond, pReq.onTimeout, nil)
			this.pendingReqs[seqno] = pReq
		}
		return err
	}()

	if nil != err {
		//返回错误响应
		logger.Infoln("send to kvnode error", err.Error())
	}
}

func (this *reqProcessor) onResp(seqno int64, resp *kendynet.ByteBuffer) {
	this.Lock()
	defer this.Unlock()
	req, ok := this.pendingReqs[seqno]
	if ok {
		//先删除定时器
		if req.deadlineTimer.Cancel() {
			delete(this.pendingReqs, seqno)
			//用oriSeqno替换seqno
			resp.PutInt64(5, req.oriSeqno)
			if err := req.session.SendMessage(resp); nil != err {
				logger.Infoln("send resp to client error", err.Error())
			}
		} else {
			logger.Infoln("cancel timer failed")
		}
	} else {
		logger.Infoln("on kvnode response but req timeout", seqno)
	}
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func NewKVProxy() *kvproxy {

	var err error
	proxy := &kvproxy{
		respChan: make(chan *kendynet.ByteBuffer, 10000),
	}

	if proxy.listener, err = net.NewListener("tcp", GetConfig().Host, verifyLogin); nil != err {
		return nil
	}

	proxy.router = newReqRounter(proxy)
	proxy.processors = []*reqProcessor{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		proxy.processors = append(proxy.processors, newReqProcessor(proxy.router))
	}

	return proxy
}

func (this *kvproxy) Start() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	for i := 0; i < runtime.NumCPU()*2; i++ {
		go func() {
			for {
				v, ok := <-this.respChan
				if !ok {
					return
				}
				if seqno, err := v.GetInt64(5); nil == err {
					processor := this.processors[seqno%int64(len(this.processors))]
					processor.onResp(seqno, v)
				} else {
					logger.Infoln("onResp but get seqno failed")
				}
			}
		}()
	}

	return this.listener.Serve(func(session kendynet.StreamSession, compress bool) {
		go func() {
			session.SetRecvTimeout(protocol.PingTime * 2)
			session.SetUserData(compress)
			session.SetReceiver(NewReceiver())
			session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))
			session.Start(func(event *kendynet.Event) {
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					seqno := atomic.AddInt64(&this.seqno, 1)
					processor := this.processors[seqno%int64(len(this.processors))]
					processor.onReq(seqno, session, event.Data.(*kendynet.ByteBuffer))
				}
			})
		}()
	})
}
