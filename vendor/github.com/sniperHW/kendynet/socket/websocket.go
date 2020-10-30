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
	*SocketBase
	conn *gorilla.Conn
}

func (this *WebSocket) sendMessage(msg kendynet.Message) error {
	if msg == nil {
		return kendynet.ErrInvaildBuff
	} else if (this.flag&closed) > 0 || (this.flag&wclosed) > 0 {
		return kendynet.ErrSocketClose
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
			if msg.Type() == message.WSBinaryMessage || msg.Type() == message.WSTextMessage {
				if timeout > 0 {
					this.conn.SetWriteDeadline(time.Now().Add(timeout))
					err = this.conn.WriteMessage(msg.Type(), msg.Bytes())
					this.conn.SetWriteDeadline(time.Time{})
				} else {
					err = this.conn.WriteMessage(msg.Type(), msg.Bytes())
				}

			} else if msg.Type() == message.WSCloseMessage || msg.Type() == message.WSPingMessage || msg.Type() == message.WSPingMessage {
				var deadline time.Time
				if timeout > 0 {
					deadline = time.Now().Add(timeout)
				}
				err = this.conn.WriteControl(msg.Type(), msg.Bytes(), deadline)
			}

			if err != nil && msg.Type() != message.WSCloseMessage {
				if this.sendQue.Closed() {
					return
				}

				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrSendTimeout
				} else {
					kendynet.GetLogger().Errorf("websocket write error:%s\n", err.Error())
					this.mutex.Lock()
					this.flag |= wclosed
					this.mutex.Unlock()
				}

				event := &kendynet.Event{Session: this, EventType: kendynet.EventTypeError, Data: err}
				this.onEvent(event)
			}
		}
	}
}

func (this *WebSocket) Close(reason string, delay time.Duration) {
	this.mutex.Lock()
	if (this.flag & closed) > 0 {
		this.mutex.Unlock()
		return
	}

	this.closeReason = reason
	this.flag |= (closed | rclosed)
	if (this.flag & wclosed) > 0 {
		delay = 0 //写端已经关闭忽略delay参数
	} else {
		delay = delay * time.Second
	}

	if delay > 0 {
		this.shutdownRead()
		msg := gorilla.FormatCloseMessage(1000, reason)
		this.sendQue.AddNoWait(message.NewWSMessage(message.WSCloseMessage, msg))
		this.sendQue.Close()
		ticker := time.NewTicker(delay)
		if (this.flag & started) == 0 {
			go this.sendThreadFunc()
		}
		this.mutex.Unlock()
		go func() {
			select {
			case <-this.sendCloseChan:
			case <-ticker.C:
			}
			ticker.Stop()
			this.doClose()
			return
		}()
	} else {
		this.sendQue.Close()
		this.mutex.Unlock()
		this.doClose()
		return
	}
}

func NewWSSocket(conn *gorilla.Conn) kendynet.StreamSession {
	if nil == conn {
		return nil
	} else {
		conn.SetCloseHandler(func(code int, text string) error {
			return fmt.Errorf("peer close reason[%s]", text)
		})
		s := &WebSocket{
			conn: conn,
		}
		s.SocketBase = &SocketBase{
			sendQue:       util.NewBlockQueue(1024),
			sendCloseChan: make(chan struct{}),
			imp:           s,
		}
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
