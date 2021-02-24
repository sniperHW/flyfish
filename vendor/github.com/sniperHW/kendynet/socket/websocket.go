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
	"errors"
	"runtime"
	"time"
)

var ErrInvaildWSMessage = fmt.Errorf("invaild websocket message")

/*
*   无封包结构，直接将收到的所有数据返回
 */

type defaultWSInBoundProcessor struct {
}

func (this *defaultWSInBoundProcessor) ReceiveAndUnpack(sess kendynet.StreamSession) (interface{}, error) {
	mt, msg, err := sess.(*WebSocket).Read()
	if err != nil {
		return nil, err
	} else {
		return message.NewWSMessage(mt, msg), nil
	}
}

func (this *defaultWSInBoundProcessor) GetRecvBuff() []byte {
	return nil
}

func (this *defaultWSInBoundProcessor) OnData([]byte) {

}

func (this *defaultWSInBoundProcessor) Unpack() (interface{}, error) {
	return nil, nil
}

func (this *defaultWSInBoundProcessor) OnSocketClose() {

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
	this.sendOnce.Do(func() {
		this.ioWait.Add(1)
		go this.sendThreadFunc()
	})
	return nil
}

func (this *WebSocket) sendThreadFunc() {
	defer this.ioWait.Done()

	for {

		timeout := this.getSendTimeout()

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

			if err != nil && !this.testFlag(fclosed) {

				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrSendTimeout
				}

				if nil != this.errorCallback {
					if err != kendynet.ErrSendTimeout {
						this.Close(err, 0)
					}
					this.errorCallback(this, err)
				} else {
					this.Close(err, 0)
				}

				if this.testFlag(fclosed) {
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
			s.Close(errors.New("gc"), 0)
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

func (this *WebSocket) GetNetConn() net.Conn {
	return this.conn.UnderlyingConn()
}

func (this *WebSocket) Read() (messageType int, p []byte, err error) {
	return this.conn.ReadMessage()
}

func (this *WebSocket) defaultInBoundProcessor() kendynet.InBoundProcessor {
	return &defaultWSInBoundProcessor{}
}

func (this *WebSocket) SendWSClose(reason string) error {
	return this.sendMessage(message.NewWSMessage(message.WSCloseMessage, gorilla.FormatCloseMessage(1000, reason)))
}
