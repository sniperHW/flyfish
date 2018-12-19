package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	"sync/atomic"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/stream_socket/tcp"
)

var (
	server     listener
	started    int32
)

type Dispatcher interface {
	Dispatch(kendynet.StreamSession,*codec.Message)
	OnClose(kendynet.StreamSession,string)
	OnNewClient(kendynet.StreamSession)
}

type listener interface {
	Close()
	Start() error
}

type tcpListener struct {
	l *tcp.Listener
}

func newTcpListener(nettype, service string) (*tcpListener, error) {
	var err error
	l := &tcpListener{}
	l.l, err = tcp.NewListener(nettype, service)

	if nil == err {
		return l, nil
	} else {
		return nil, err
	}
}

func (this *tcpListener) Close() {
	
	this.l.Close()
}

func (this *tcpListener) Start() error {
	if nil == this.l {
		return fmt.Errorf("invaild listener")
	}
	return this.l.Start(func(session kendynet.StreamSession) {

		//fmt.Println("new client")

		//session.SetRecvTimeout(common.HeartBeat_Timeout * time.Second)
		session.SetReceiver(codec.NewReceiver())
		session.SetEncoder(codec.NewEncoder())
		session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
			//fmt.Println("close callback")
			onClose(sess, reason)
		})
		onNewClient(session)
		session.Start(func(event *kendynet.Event) {
			if event.EventType == kendynet.EventTypeError {
				//fmt.Println("on error")
				event.Session.Close(event.Data.(error).Error(), 0)
			} else {
				msg := event.Data.(*codec.Message)
				dispatch(session,msg)
			}
		})
	})
}

func StartTcpServer(nettype, service string) error {
	l, err := newTcpListener(nettype, service)
	if nil != err {
		return err
	}
	return startServer(l)
}

func startServer(l listener) error {
	if !atomic.CompareAndSwapInt32(&started, 0, 1) {
		return fmt.Errorf("server already started")
	}
	server = l
	go func() {
		err := server.Start()
		if nil != err {
			Errorf("server.Start() error:%s\n", err.Error())
		}
	}()

	return nil
}

func StopServer() {
	if !atomic.CompareAndSwapInt32(&started, 1, 0) {
		return
	}
	server.Close()
}


func Stop() {
	StopServer()
}
