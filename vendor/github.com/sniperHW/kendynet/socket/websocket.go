/*
*  websocket会话
 */

package socket

import (
	"fmt"
	gorilla "github.com/gorilla/websocket"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/message"
	"github.com/sniperHW/kendynet/util"
	"net"
	//"sync/atomic"
	"runtime"
	"time"
)

var ErrInvaildWSMessage = fmt.Errorf("invaild websocket message")

/*
*   无封包结构，直接将收到的所有数据返回
 */

type defaultWSReceiver struct {
}

func (this *defaultWSReceiver) ReceiveAndUnpack(sess kendynet.StreamSession) (interface{}, error) {
	mt, msg, err := sess.(*WebSocket).Read()
	if err != nil {
		return nil, err
	} else {
		return message.NewWSMessage(mt, msg), nil
	}
}

type WebSocket struct {
	SocketBase
	conn *gorilla.Conn
}

func (this *WebSocket) sendMessage(msg kendynet.Message) error {
	if msg == nil {
		return kendynet.ErrInvaildBuff
	} else {
		switch msg.(type) {
		case *message.WSMessage:
			fullReturn := true
			err := this.sendQue.AddNoWait(msg, fullReturn)
			if nil != err {
				if err == util.ErrQueueClosed {
					err = kendynet.ErrSocketClose
				} else if err == util.ErrQueueFull {
					err = kendynet.ErrSendQueFull
				}
				return err
			}
			break
		default:
			return ErrInvaildWSMessage
		}
	}
	return nil
}

func (this *WebSocket) sendThreadFunc() {

	defer func() {
		close(this.sendCloseChan)
		this.setFlag(fsendStoped)
		if this.testFlag(frecvStoped) {
			if onClose := this.onClose.Load(); nil != onClose {
				onClose.(func(kendynet.StreamSession, string))(this.imp.(kendynet.StreamSession), this.closeReason)
			}
		}
	}()

	timeout := this.getSendTimeout()
	for {
		closed, localList := this.sendQue.Get()
		size := len(localList)
		if closed && size == 0 {
			break
		}

		for i := 0; i < size; i++ {
			var err error
			msg := localList[i].(*message.WSMessage)

			if timeout > 0 {
				this.conn.SetWriteDeadline(time.Now().Add(timeout))
				err = this.conn.WriteMessage(msg.Type(), msg.Bytes())
				this.conn.SetWriteDeadline(time.Time{})
			} else {
				err = this.conn.WriteMessage(msg.Type(), msg.Bytes())
			}

			if err != nil && msg.Type() != message.WSCloseMessage {
				breakLoop := false
				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrSendTimeout
				} else {
					breakLoop = true
					this.sendQue.CloseAndClear()
				}

				if this.callEventCB(&kendynet.Event{Session: this, EventType: kendynet.EventTypeError, Data: err}, fclosed) || breakLoop {
					return
				}
			}
		}
	}
}

func NewWSSocket(conn *gorilla.Conn) kendynet.StreamSession {
	if nil == conn {
		return nil
	} else {

		conn.SetCloseHandler(func(code int, text string) error {
			conn.UnderlyingConn().Close()
			return nil
		})

		s := &WebSocket{
			conn: conn,
		}
		s.SocketBase = SocketBase{
			sendQue:       util.NewBlockQueue(1024),
			sendCloseChan: make(chan struct{}),
			imp:           s,
		}

		runtime.SetFinalizer(s, func(s *WebSocket) {
			s.Close("gc", 0)
		})

		return s
	}
}

func (this *WebSocket) SetPingHandler(h func(appData string) error) {
	this.conn.SetPingHandler(h)
}

func (this *WebSocket) SetPongHandler(h func(appData string) error) {
	this.conn.SetPongHandler(h)
}

func (this *WebSocket) GetUnderConn() interface{} {
	return this.conn
}

func (this *WebSocket) getNetConn() net.Conn {
	return this.conn.UnderlyingConn()
}

func (this *WebSocket) Read() (messageType int, p []byte, err error) {
	return this.conn.ReadMessage()
}

func (this *WebSocket) defaultReceiver() kendynet.Receiver {
	return &defaultWSReceiver{}
}

func (this *WebSocket) SendWSClose(reason string) error {
	return this.sendMessage(message.NewWSMessage(message.WSCloseMessage, gorilla.FormatCloseMessage(1000, reason)))
}
