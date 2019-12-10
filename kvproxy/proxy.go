package kvproxy

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/codec/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"github.com/sniperHW/kendynet/timer"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type pendingReq struct {
	seqno         int64
	oriSeqno      int64
	session       kendynet.StreamSession
	deadlineTimer *timer.Timer
	processor     *reqProcessor
}

var reqCount int64
var respCount int64

type kvproxy struct {
	router     *reqRouter
	processors []*reqProcessor
	listener   *tcp.Listener
	seqno      int64
}

func (this *pendingReq) onTimeout(_ *timer.Timer) {
	this.processor.Lock()
	defer this.processor.Unlock()
	Infoln("remove timeout req", this.seqno)
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
		timerMgr:    timer.NewTimerMgr(),
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

	if oriSeqno, err = req.GetInt64(5); nil != err {
		return
	}

	if lenUnikey, err = req.GetInt16(21); nil != err {
		return
	}

	if 0 == lenUnikey {
		return
	}

	if unikey, err = req.GetString(23, uint64(lenUnikey)); nil != err {
		return
	}

	if timeout, err = req.GetUint32(17); nil != err {
		return
	}

	if cmd, err = req.GetUint16(23 + uint64(lenUnikey)); nil != err {
		return
	}

	if cmd == uint16(protocol.CmdType_Ping) {
		//返回心跳
		//pbdata,
		//resp := codec.NewMessage(codec.CommonHead{Seqno: oriSeqno}, &protocol.PingResp{
		//	Timestamp: req.GetTimestamp(),
		//})
		//session.Send(resp)
		return
	}

	atomic.AddInt64(&reqCount, 1)

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
			pReq.deadlineTimer = this.timerMgr.Once(time.Duration(timeout)*time.Millisecond, nil, pReq.onTimeout)
			this.pendingReqs[seqno] = pReq
		}
		return err
	}()

	if nil != err {
		//返回错误响应
		Infoln("send to kvnode error", err.Error())
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
				Infoln("send resp to client error", err.Error())
			}
		} else {
			Infoln("cancel timer failed")
		}
	} else {
		Infoln("on kvnode response but req timeout", seqno)
	}
}

func sendLoginResp(session kendynet.StreamSession, loginResp *protocol.LoginResp) bool {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginResp)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func recvLoginReq(session kendynet.StreamSession) (*protocol.LoginReq, error) {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginReq := &protocol.LoginReq{}
			if err = proto.Unmarshal(buffer[2:w], loginReq); err != nil {
				return loginReq, nil
			} else {
				return nil, err
			}
		}
	}
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func NewKVProxy() *kvproxy {

	var err error
	proxy := &kvproxy{}

	if proxy.listener, err = tcp.New("tcp", GetConfig().Host); nil != err {
		return nil
	}

	proxy.router = newReqRounter(proxy)
	proxy.processors = []*reqProcessor{}
	for i := 0; i < 31; i++ {
		proxy.processors = append(proxy.processors, newReqProcessor(proxy.router))
	}

	return proxy
}

func (this *kvproxy) onResp(resp *kendynet.ByteBuffer) {
	atomic.AddInt64(&respCount, 1)
	if seqno, err := resp.GetInt64(5); nil == err {
		processor := this.processors[seqno%int64(len(this.processors))]
		processor.onResp(seqno, resp)
	} else {
		Infoln("onResp but get seqno failed")
	}
}

func (this *kvproxy) Start() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession) {
		go func() {
			loginReq, err := recvLoginReq(session)
			if nil != err {
				session.Close("login failed", 0)
				return
			}

			if !verifyLogin(loginReq) {
				session.Close("login failed", 0)
				return
			}

			loginResp := &protocol.LoginResp{
				Ok:       true,
				Compress: loginReq.GetCompress(),
			}

			if !sendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			session.SetUserData(loginReq.GetCompress())

			session.SetReceiver(NewReceiver())
			session.SetEncoder(codec.NewEncoder(pb.GetNamespace("response"), loginResp.GetCompress()))

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

func init() {
	timer.Repeat(time.Second, nil, func(_ *timer.Timer) {
		Infoln("reqCount", atomic.LoadInt64(&reqCount), "respCount", atomic.LoadInt64(&respCount))
	})
}
