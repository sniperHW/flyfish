package net

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var maxSendBuffSize int = 65535

func init() {
	InitLogger(logger.NewZapLogger("net.log", "./log", "debug", 100, 14, 10, true))
}

type encoder struct {
}

func (this *encoder) EnCode(o interface{}, b *buffer.Buffer) error {
	switch o.(type) {
	case string:
		b.AppendString(o.(string))
	case []byte:
		b.AppendBytes(o.([]byte))
	default:
		return errors.New("invaild o")
	}
	return nil
}

type errencoder struct {
}

func (this *errencoder) EnCode(o interface{}, b *buffer.Buffer) error {
	return errors.New("invaild o")
}

type TestInboundProcessor struct {
	buffer []byte
	w      int
}

func (this *TestInboundProcessor) GetRecvBuff() []byte {
	return this.buffer[this.w:]
}

func (this *TestInboundProcessor) OnData(data []byte) {
	this.w += len(data)
}

func (this *TestInboundProcessor) Unpack() (interface{}, error) {
	if this.w == 0 {
		return nil, nil
	} else {
		o := make([]byte, 0, this.w)
		o = append(o, this.buffer[:this.w]...)
		this.w = 0
		return o, nil
	}
}

func (this *TestInboundProcessor) OnSocketClose() {

}

func TestSendTimeout(t *testing.T) {

	{

		tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

		listener, _ := net.ListenTCP("tcp", tcpAddr)

		var mu sync.Mutex

		var holdSession *Socket

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				} else {
					conn.(*net.TCPConn).SetReadBuffer(0)
					mu.Lock()
					holdSession = NewSocket(conn, OutputBufLimit{})
					mu.Unlock()
					//不启动接收
				}
			}
		}()

		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		conn.(*net.TCPConn).SetWriteBuffer(0)
		session := NewSocket(conn, OutputBufLimit{})

		die := make(chan struct{})

		session.SetCloseCallBack(func(sess *Socket, reason error) {
			close(die)
		})

		session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

		session.SetEncoder(&encoder{})

		session.SetSendTimeout(time.Second)

		triger := false

		session.SetErrorCallBack(func(sess *Socket, err error) {
			assert.Equal(t, ErrSendTimeout, err)
			if triger { //第二次触发再close
				fmt.Println("here")
				sess.Close(err, 0)
			} else {
				triger = true
			}
		})

		session.BeginRecv(func(s *Socket, msg interface{}) {
		})

		go func() {
			for {
				err := session.Send(strings.Repeat("a", 65535))
				if nil != err {
					fmt.Println("break here", err)
					break
				}
			}
		}()
		<-die
		mu.Lock()
		holdSession.Close(nil, 0)
		mu.Unlock()

		listener.Close()
	}
}

func TestSocket(t *testing.T) {

	{

		tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

		listener, _ := net.ListenTCP("tcp", tcpAddr)

		die := make(chan struct{})

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				} else {
					session := NewSocket(conn, OutputBufLimit{})
					session.GetNetConn()
					session.SetEncoder(&encoder{})
					session.SetRecvTimeout(time.Second * 1).SetSendTimeout(time.Second * 1)
					session.SetCloseCallBack(func(s *Socket, reason error) {
						fmt.Println("server close")
						close(die)
					}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

					session.SetErrorCallBack(func(s *Socket, err error) {
						fmt.Println("err", err)
						s.Close(err, 0)
						assert.Equal(t, s.Send("hello"), ErrSocketClose)
					}).BeginRecv(func(s *Socket, msg interface{}) {
						fmt.Println("recv", string(msg.([]byte)))
						s.Send(msg)
					})
				}
			}
		}()

		fmt.Println("00")
		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		session := NewSocket(conn, OutputBufLimit{})

		respChan := make(chan interface{})

		session.SetUserData(1)
		assert.Equal(t, 1, session.GetUserData().(int))

		session.SetCloseCallBack(func(sess *Socket, reason error) {
			fmt.Println("client close")
		}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)}).SetEncoder(&encoder{})

		session.SetErrorCallBack(func(s *Socket, err error) {
			s.Close(err, 0)
			assert.Equal(t, true, s.IsClosed())
		}).BeginRecv(func(s *Socket, msg interface{}) {
			respChan <- msg
		})

		fmt.Println("0011")

		assert.Equal(t, nil, session.Send("hello"))

		resp := <-respChan

		fmt.Println("0022")

		assert.Equal(t, resp.([]byte), []byte("hello"))

		<-die

		listener.Close()
	}
	fmt.Println("11")
	{

		tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

		listener, _ := net.ListenTCP("tcp", tcpAddr)

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				} else {
					NewSocket(conn, OutputBufLimit{}).SetEncoder(&encoder{}).
						SetRecvTimeout(time.Second * 1).
						SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)}).
						BeginRecv(func(s *Socket, msg interface{}) {
							s.Send(msg)
							s.Close(nil, time.Second)
						})
				}
			}
		}()
		fmt.Println("22")
		{
			dialer := &net.Dialer{}
			conn, _ := dialer.Dial("tcp", "localhost:8110")
			session := NewSocket(conn, OutputBufLimit{})

			respChan := make(chan interface{})

			session.SetEncoder(&encoder{})

			session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

			session.BeginRecv(func(s *Socket, msg interface{}) {
				respChan <- msg
			})

			session.Send("hello")

			resp := <-respChan

			assert.Equal(t, resp.([]byte), []byte("hello"))
		}
		fmt.Println("33")
		{
			dialer := &net.Dialer{}
			conn, _ := dialer.Dial("tcp", "localhost:8110")
			session := NewSocket(conn, OutputBufLimit{})

			session.SetEncoder(&encoder{})

			session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

			session.SetCloseCallBack(func(sess *Socket, reason error) {

			})

			session.Close(nil, 0)

			err := session.BeginRecv(func(s *Socket, msg interface{}) {

			})

			assert.Equal(t, ErrSocketClose, err)
		}

		{
			dialer := &net.Dialer{}
			conn, _ := dialer.Dial("tcp", "localhost:8110")
			session := NewSocket(conn, OutputBufLimit{})
			session.SetCloseCallBack(func(sess *Socket, reason error) {
				fmt.Println("reason", reason)
			})
			_ = session.LocalAddr()
			_ = session.RemoteAddr()
			session = nil
			for i := 0; i < 2; i++ {
				time.Sleep(time.Second)
				runtime.GC()
			}
		}

		fmt.Println("here----------")

		{
			dialer := &net.Dialer{}
			conn, _ := dialer.Dial("tcp", "localhost:8110")
			session := NewSocket(conn, OutputBufLimit{})

			die := make(chan struct{})

			session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

			session.SetEncoder(&errencoder{}).SetRecvTimeout(time.Second).BeginRecv(func(s *Socket, msg interface{}) {

			})

			session.SetCloseCallBack(func(sess *Socket, reason error) {
				fmt.Println("close", reason)
				close(die)
			})

			session.Send("hello")

			<-die
		}

		listener.Close()
	}
	fmt.Println("here----------2")
	{

		tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

		listener, _ := net.ListenTCP("tcp", tcpAddr)

		die := make(chan struct{})

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				} else {
					NewSocket(conn, OutputBufLimit{}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)}).BeginRecv(func(s *Socket, msg interface{}) {
						close(die)
					})
				}
			}
		}()

		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		session := NewSocket(conn, OutputBufLimit{})

		session.SetEncoder(&encoder{})

		session.Send("hello")

		session.Close(nil, time.Second)

		_ = <-die

		listener.Close()
	}
	fmt.Println("here----------3")
	{

		tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

		listener, _ := net.ListenTCP("tcp", tcpAddr)

		serverdie := make(chan struct{})
		clientdie := make(chan struct{})

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				} else {
					session := NewSocket(conn, OutputBufLimit{})
					session.SetRecvTimeout(time.Second * 1)
					session.SetSendTimeout(time.Second * 1)
					session.SetCloseCallBack(func(sess *Socket, reason error) {
						fmt.Println("server die")
						close(serverdie)
					})
					session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})
					session.BeginRecv(func(s *Socket, msg interface{}) {
					})

				}
			}
		}()

		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		session := NewSocket(conn, OutputBufLimit{})
		session.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})
		session.SetEncoder(&encoder{}).SetCloseCallBack(func(sess *Socket, reason error) {
			fmt.Println("client die")
			close(clientdie)
		}).BeginRecv(func(s *Socket, msg interface{}) {
		})

		go func() {
			for {
				if err := session.Send("hello"); nil != err {
					if err == ErrSocketClose {
						break
					}
				}
			}
		}()

		go func() {
			time.Sleep(time.Second * 2)
			session.Close(nil, time.Second)
		}()

		<-clientdie
		<-serverdie

		listener.Close()
	}

}

func TestShutDownRead(t *testing.T) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

	listener, _ := net.ListenTCP("tcp", tcpAddr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			} else {
				NewSocket(conn, OutputBufLimit{}).SetCloseCallBack(func(sess *Socket, reason error) {
					fmt.Println("server close")
				}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)}).BeginRecv(func(s *Socket, msg interface{}) {
				})
			}
		}
	}()

	dialer := &net.Dialer{}
	conn, _ := dialer.Dial("tcp", "localhost:8110")
	session := NewSocket(conn, OutputBufLimit{})

	die := make(chan struct{})

	session.SetCloseCallBack(func(sess *Socket, reason error) {
		fmt.Println("client close", reason)
		close(die)
	}).SetEncoder(&encoder{}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)}).SetRecvTimeout(time.Second * 1)

	session.BeginRecv(func(s *Socket, msg interface{}) {
	})

	fmt.Println("ShutdownRead1")

	session.ShutdownRead()

	fmt.Println("ShutdownRead2")

	time.Sleep(time.Second * 2)

	session.Close(nil, 0)

	<-die

	listener.Close()

}

func TestShutDownWrite(t *testing.T) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "localhost:8110")

	listener, _ := net.ListenTCP("tcp", tcpAddr)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			} else {
				s := NewSocket(conn, OutputBufLimit{}).SetCloseCallBack(func(sess *Socket, reason error) {
					fmt.Printf("server close %p\n", sess)
				}).SetErrorCallBack(func(sess *Socket, reason error) {
					if reason == io.EOF {
						fmt.Println("send ")
						fmt.Println(sess.Send("hello"))
					}
					sess.Close(nil, time.Second)
				})

				s.SetEncoder(&encoder{})
				s.SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})
				s.BeginRecv(func(s *Socket, msg interface{}) {
					fmt.Println("ondata", string(msg.([]byte)))
				})
			}
		}
	}()

	fmt.Println("11111111111")

	{
		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		session := NewSocket(conn, OutputBufLimit{}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

		die := make(chan struct{})

		session.SetCloseCallBack(func(sess *Socket, reason error) {
			fmt.Println("client close", reason)
		}).SetEncoder(&encoder{}).SetRecvTimeout(time.Second * 1)

		session.BeginRecv(func(s *Socket, msg interface{}) {
			assert.Equal(t, "hello", string(msg.([]byte)))
			close(die)
		})

		session.ShutdownWrite()

		<-die
	}
	fmt.Println("22222222222222")
	{
		dialer := &net.Dialer{}
		conn, _ := dialer.Dial("tcp", "localhost:8110")
		session := NewSocket(conn, OutputBufLimit{}).SetInBoundProcessor(&TestInboundProcessor{buffer: make([]byte, 1024)})

		die := make(chan struct{})

		session.SetCloseCallBack(func(sess *Socket, reason error) {
			fmt.Println("client close", reason)
		}).SetEncoder(&encoder{}).SetRecvTimeout(time.Second * 1)

		session.BeginRecv(func(s *Socket, msg interface{}) {
			assert.Equal(t, "hello", string(msg.([]byte)))
			close(die)
		})

		session.Send("hello")

		session.ShutdownWrite()

		<-die
	}

	listener.Close()

}
