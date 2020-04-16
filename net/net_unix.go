// +build linux darwin netbsd freebsd openbsd dragonfly
package net

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/aio"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type BufferPool struct {
	pool sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4*1024*1024)
			},
		},
	}
}

func (p *BufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *BufferPool) Put(buff []byte) {
	if cap(buff) != 4*1024*1024 {
		p.pool.Put(buff[:cap(buff)])
	}
}

var aioService *aio.AioService
var buffPool *BufferPool

func init() {
	buffPool = NewBufferPool()
	aioService = aio.NewAioService(1, runtime.NumCPU()*2, runtime.NumCPU()*2, buffPool)
}

type Listener struct {
	l           *net.TCPListener
	started     int32
	closed      int32
	verifyLogin func(*protocol.LoginReq) bool
}

func NewListener(nettype, service string, verifyLogin func(*protocol.LoginReq) bool) (*Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr(nettype, service)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP(nettype, tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Listener{l: l, verifyLogin: verifyLogin}, nil
}

func (this *Listener) Close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		if nil != this.l {
			this.l.Close()
		}
	}
}

func (this *Listener) Serve(onNewClient func(kendynet.StreamSession, bool)) error {

	if nil == onNewClient {
		return kendynet.ErrInvaildNewClientCB
	}

	if !atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		return kendynet.ErrServerStarted
	}

	for {
		conn, err := this.l.Accept()
		if err != nil {
			if atomic.LoadInt32(&this.closed) == 1 {
				return nil
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				kendynet.GetLogger().Errorf("accept temp err: %v", ne)
				continue
			} else {
				return err
			}

		} else {
			go func() {

				loginReq, err := login.RecvLoginReq(conn.(*net.TCPConn))
				if nil != err {
					conn.Close()
					return
				}

				if !this.verifyLogin(loginReq) {
					conn.Close()
					return
				}

				loginResp := &protocol.LoginResp{
					Ok:       true,
					Compress: loginReq.GetCompress(),
				}

				if !login.SendLoginResp(conn.(*net.TCPConn), loginResp) {
					conn.Close()
					return
				}

				onNewClient(aio.NewAioSocket(aioService, conn), loginReq.GetCompress())

			}()
		}
	}
}

type Connector struct {
	nettype  string
	addr     string
	compress bool
}

func NewConnector(nettype string, addr string, compress bool) *Connector {
	return &Connector{nettype: nettype, addr: addr, compress: compress}
}

func (this *Connector) Dial(timeout time.Duration) (kendynet.StreamSession, bool, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial(this.nettype, this.addr)
	if err != nil {
		return nil, false, err
	}

	if !login.SendLoginReq(conn.(*net.TCPConn), &protocol.LoginReq{Compress: this.compress}) {
		conn.Close()
		return nil, false, fmt.Errorf("login failed")
	}

	loginResp, err := login.RecvLoginResp(conn.(*net.TCPConn))
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return nil, false, fmt.Errorf("login failed")
	}

	return aio.NewAioSocket(aioService, conn), loginResp.GetCompress(), nil
}

type Receiver struct {
	buffer         []byte
	w              uint64
	r              uint64
	nextPacketSize uint64
	unCompressor   UnCompressorI
	pbSpace        *pb.Namespace
}

func NewReceiver(pbSpace *pb.Namespace, compress bool) *Receiver {
	receiver := &Receiver{
		pbSpace: pbSpace,
	}
	receiver.buffer = make([]byte, initBufferSize)
	if compress {
		receiver.unCompressor = &ZipUnCompressor{}
	}
	return receiver
}

func (this *Receiver) OnRecvOk(s kendynet.StreamSession, buff []byte) {
	if nil == this.buffer {
		this.buffer = buff
		this.r = 0
		this.w = uint64(len(buff))
	} else {
		l := uint64(len(buff))
		space := uint64(len(this.buffer)) - (this.w - this.r)
		if space < l {
			buffer := make([]byte, (this.w-this.r)+l)
			copy(buffer, this.buffer[this.r:this.w])
			copy(buffer[:this.w-this.r], buff[:l])
			buffPool.Put(this.buffer)
			this.buffer = buffer
			this.w = (this.w - this.r) + l
			this.r = 0
		} else {
			if uint64(len(this.buffer))-this.w >= l {
				copy(this.buffer[:this.w], buff[:l])
				this.w += l
			} else {
				copy(this.buffer, this.buffer[this.r:this.w])
				this.w -= this.r
				copy(this.buffer[:this.w], buff[:l])
				this.w += l
				this.r = 0
			}
			buffPool.Put(buff)
		}
	}
}

func (this *Receiver) StartReceive(s kendynet.StreamSession) {
	s.(*aio.AioSocket).Recv(nil)
}

func (this *Receiver) OnClose() {
	if nil != this.buffer {
		buffPool.Put(this.buffer)
		this.buffer = nil
	}
}

func (this *Receiver) unPack() (ret interface{}, err error) {
	unpackSize := uint64(this.w - this.r)
	if unpackSize > minSize {
		var payload uint32
		var cmd uint16
		var buff []byte
		var msg proto.Message
		var totalSize uint64
		var flag byte

		reader := kendynet.NewReader(kendynet.NewByteBuffer(this.buffer[this.r:], unpackSize))
		if payload, err = reader.GetUint32(); err != nil {
			return
		}

		if uint64(payload) == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if uint64(payload)+SizeLen > conf.MaxPacketSize {
			err = fmt.Errorf("large packet %d", uint64(payload)+SizeLen)
			return
		}

		totalSize = uint64(payload + SizeLen)

		this.nextPacketSize = totalSize

		if totalSize <= unpackSize {

			if flag, err = reader.GetByte(); err != nil {
				return
			}

			//read head
			var head CommonHead
			var sizeOfUniKey int16

			if head.Seqno, err = reader.GetInt64(); err != nil {
				return
			}

			if head.ErrCode, err = reader.GetInt32(); err != nil {
				return
			}

			if head.Timeout, err = reader.GetUint32(); err != nil {
				return
			}

			if sizeOfUniKey, err = reader.GetInt16(); err != nil {
				return
			}

			if sizeOfUniKey > 0 {
				if head.UniKey, err = reader.GetString(uint64(sizeOfUniKey)); err != nil {
					return
				}
			}

			if cmd, err = reader.GetUint16(); err != nil {
				return
			}
			sizeOfHead := 8 + 4 + 4 + 2 + uint32(sizeOfUniKey)
			//普通消息
			size := payload - SizeCmd - SizeFlag - sizeOfHead
			if buff, err = reader.GetBytes(uint64(size)); err != nil {
				return
			}

			if flag == byte(1) {
				if nil == this.unCompressor {
					err = fmt.Errorf("invaild compress packet")
					return
				}

				if buff, err = this.unCompressor.UnCompress(buff); err != nil {
					return
				}
			}

			if msg, err = this.pbSpace.Unmarshal(uint32(cmd), buff); err != nil {
				return
			}
			this.nextPacketSize = 0
			this.r += totalSize
			ret = NewMessageWithCmd(uint16(cmd), head, msg)
		}
	}
	return
}

func (this *Receiver) ReceiveAndUnpack(s kendynet.StreamSession) (interface{}, error) {
	msg, err := this.unPack()
	if nil == msg && nil == err {
		return nil, s.(*aio.AioSocket).Recv(this.buffer)
	}

	if nil != msg && this.r == this.w {
		buffPool.Put(this.buffer)
		this.buffer = nil
	}

	return msg, err
}
