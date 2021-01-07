/*
*  tcp或unix域套接字会话
 */

package socket

import (
	"bufio"
	//"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"net"
	"runtime"
	"time"
)

type defaultSSReceiver struct {
	buffer []byte
}

func (this *defaultSSReceiver) ReceiveAndUnpack(sess kendynet.StreamSession) (interface{}, error) {
	n, err := sess.(*StreamSocket).Read(this.buffer[:])
	if err != nil {
		return nil, err
	}
	msg := kendynet.NewByteBuffer(n)
	msg.AppendBytes(this.buffer[:n])
	return msg, err
}

type StreamSocket struct {
	SocketBase
	conn net.Conn
}

func (this *StreamSocket) sendMessage(msg kendynet.Message) error {
	if msg == nil {
		return kendynet.ErrInvaildBuff
	} else {
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
	}
	return nil
}

func (this *StreamSocket) sendThreadFunc() {
	defer func() {
		close(this.sendCloseChan)
		this.setFlag(fsendStoped)
		if this.testFlag(frecvStoped) {
			this.clearup()
		}
	}()

	var err error

	writer := bufio.NewWriterSize(this.conn, kendynet.SendBufferSize)

	timeout := this.getSendTimeout()

	for {

		closed, localList := this.sendQue.Get()
		size := len(localList)
		if closed && size == 0 {
			break
		}

		for i := 0; i < size; i++ {
			msg := localList[i].(kendynet.Message)

			data := msg.Bytes()
			for data != nil || (i == (size-1) && writer.Buffered() > 0) {
				if data != nil {
					var s int
					if len(data) > writer.Available() {
						s = writer.Available()
					} else {
						s = len(data)
					}
					writer.Write(data[:s])

					if s != len(data) {
						data = data[s:]
					} else {
						data = nil
					}
				}

				if writer.Available() == 0 || i == (size-1) {
					if timeout > 0 {
						this.conn.SetWriteDeadline(time.Now().Add(timeout))
						err = writer.Flush()
						this.conn.SetWriteDeadline(time.Time{})
					} else {
						err = writer.Flush()
					}
					if err != nil {

						breakLoop := false
						if kendynet.IsNetTimeout(err) {
							err = kendynet.ErrSendTimeout
						} else {
							breakLoop = true
							this.sendQue.CloseAndClear()
						}

						if this.callEventCB(&kendynet.Event{Session: this, EventType: kendynet.EventTypeError, Data: err}) || breakLoop {
							return
						}
					}
				}
			}
		}
	}
}

func NewStreamSocket(conn net.Conn) kendynet.StreamSession {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	s := &StreamSocket{
		conn: conn,
	}
	s.SocketBase = SocketBase{
		sendQue:       util.NewBlockQueue(1024),
		sendCloseChan: make(chan struct{}),
		imp:           s,
	}

	//fmt.Println("new")

	runtime.SetFinalizer(s, func(s *StreamSocket) {
		//fmt.Println("gc")
		s.Close("gc", 0)
	})

	return s
}

func (this *StreamSocket) Read(b []byte) (int, error) {
	return this.conn.Read(b)
}

func (this *StreamSocket) getNetConn() net.Conn {
	return this.conn
}

func (this *StreamSocket) GetUnderConn() interface{} {
	return this.getNetConn()
}

func (this *StreamSocket) defaultReceiver() kendynet.Receiver {
	return &defaultSSReceiver{buffer: make([]byte, 4096)}
}

/*
func (this *StreamSocket) sendThreadFunc() {

	defer func() {
		close(this.sendCloseChan)
	}()

	var err error

	var sendBuff bytes.Buffer

	cap := 1024 * 1024

	timeout := this.sendTimeout

	for {

		closed, localList := this.sendQue.Get()
		size := len(localList)
		if closed && size == 0 {
			break
		}

		if sendBuff.Cap() > cap {
			sendBuff = bytes.Buffer{}
		} else {
			sendBuff.Reset()
		}

		for i := 0; i < size; i++ {
			msg := localList[i].(kendynet.Message)
			data := msg.Bytes()
			sendBuff.Write(data)
		}

		n := 0
		b := sendBuff.Bytes()

		for {

			if timeout > 0 {
				this.conn.SetWriteDeadline(time.Now().Add(timeout))
				n, err = this.conn.Write(b)
				this.conn.SetWriteDeadline(time.Time{})
			} else {
				n, err = this.conn.Write(b)
			}

			if nil != err {
				if this.sendQue.Closed() {
					return
				}
				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrSendTimeout
				} else {
					kendynet.Errorf("writer.Flush error:%s\n", err.Error())
					this.mutex.Lock()
					this.flag |= wclosed
					this.mutex.Unlock()
				}
				event := &kendynet.Event{Session: this, EventType: kendynet.EventTypeError, Data: err}
				this.onEvent(event)
				if this.sendQue.Closed() {
					return
				}
			} else {
				if n == len(b) {
					break
				} else {
					b = b[n:]
				}
			}
		}
	}
}
*/

/*
func (this *StreamSocket) sendThreadFunc() {

	defer func() {
		close(this.sendCloseChan)
	}()

	var err error

	var closed bool

	var localList []interface{}

	var writeBuffers net.Buffers

	timeout := this.sendTimeout

	totalBytes := int64(0)

	for {
		if len(writeBuffers) > 0 {
			closed, localList = this.sendQue.GetNoWait()
		} else {
			closed, localList = this.sendQue.Get()
		}
		size := len(localList)
		if closed && size == 0 {
			break
		}

		n := int64(0)

		for i := 0; i < size; i++ {
			msg := localList[i].(kendynet.Message)
			data := msg.Bytes()
			totalBytes += int64(len(data))
			writeBuffers = append(writeBuffers, data)
		}

		if timeout > 0 {
			this.conn.SetWriteDeadline(time.Now().Add(timeout))
			n, err = writeBuffers.WriteTo(this.conn)
			this.conn.SetWriteDeadline(time.Time{})
		} else {
			n, err = writeBuffers.WriteTo(this.conn)
		}

		if nil != err {
			if this.sendQue.Closed() {
				return
			}
			if kendynet.IsNetTimeout(err) {
				err = kendynet.ErrSendTimeout
			} else {
				kendynet.Errorf("writer.Flush error:%s\n", err.Error())
				this.mutex.Lock()
				this.flag |= wclosed
				this.mutex.Unlock()
			}
			event := &kendynet.Event{Session: this, EventType: kendynet.EventTypeError, Data: err}
			this.onEvent(event)
			if this.sendQue.Closed() {
				return
			}
		} else {
			if n >= totalBytes {
				writeBuffers = writeBuffers[:0]
				totalBytes = 0
			} else {
				oldBuffers := writeBuffers
				writeBuffers = net.Buffers{}
				totalBytes = 0
				for _, v := range oldBuffers {
					if n >= int64(len(v)) {
						n -= int64(len(v))
					} else {
						if n > 0 {
							v = v[n:]
							n = 0
						}
						writeBuffers = append(writeBuffers, v)
						totalBytes += int64(len(v))
					}
				}
			}
		}
	}
}*/
