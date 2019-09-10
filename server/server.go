package raft

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/conf"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	server *Server
)

type Server struct {
	listener *tcp.Listener
	stoped   int32
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

func (this *Server) isStoped() bool {
	return atomic.LoadInt32(&this.stoped) == 1
}

func (this *Server) startListener() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession) {
		go func() {

			config := conf.GetConfig()

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
				Ok:       proto.Bool(true),
				Compress: proto.Bool(config.Compress && loginReq.GetCompress()),
			}

			if !sendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			session.SetRecvTimeout(protocol.PingTime * 2)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetReceiver(codec.NewReceiver(config.Compress && loginReq.GetCompress()))
			session.SetEncoder(codec.NewEncoder(config.Compress && loginReq.GetCompress()))

			session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
				onClose(sess, reason)
			})
			onNewClient(session)
			session.Start(func(event *kendynet.Event) {
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					msg := event.Data.(*codec.Message)
					dispatch(session, msg)
				}
			})
		}()
	})
}

func (this *Server) Start(id *int, cluster *string) error {

	var err error

	config := conf.GetConfig()

	if config.DBConfig.SqlType == "mysql" {
		BinaryToSqlStr = mysqlBinaryToPgsqlStr
		buildInsertUpdateString = buildInsertUpdateStringMySql
	} else {
		BinaryToSqlStr = pgsqlBinaryToPgsqlStr
		buildInsertUpdateString = buildInsertUpdateStringPgSql
	}

	this.listener, err = tcp.New("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))

	if nil != err {
		return err
	}

	if !initSql() {
		return fmt.Errorf("initSql failed")
	}

	initKVStore(id, cluster)

	go func() {
		err := this.startListener()
		if nil != err {
			Errorf("server.Start() error:%s\n", err.Error())
		}
		Infoln("flyfish listener stop")
	}()

	Infoln("flyfish start:", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))

	return nil
}

func waitCondition(fn func() bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			if fn() {
				wg.Done()
				break
			}
		}
	}()
	wg.Wait()
}

func (this *Server) Stop() {

	if atomic.CompareAndSwapInt32(&this.stoped, 0, 1) {
		Infoln("StopServer")
		//关闭监听
		this.listener.Close()

		//关闭现有连接的读端
		sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).ShutdownRead()
			return true
		})

		Infoln("ShutdownRead ok", "cmdCount:", cmdCount)

		//等待redis请求和命令执行完成
		waitCondition(func() bool {
			if atomic.LoadInt32(&cmdCount) == 0 {
				return true
			} else {
				return false
			}
		})

		stopSql()

		Infoln("sql stop ok", "totalSqlUpdateCount:", totalSqlCount)

		//关闭所有客户连接

		sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).Close("", 1)
			return true
		})

		waitCondition(func() bool {
			if atomic.LoadInt32(&clientCount) == 0 {
				return true
			} else {
				return false
			}
		})

		caches.stop()

		Infoln("flyfish stop ok")

	}
}

func Start(id *int, cluster *string) error {
	s := &Server{}
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)((unsafe.Pointer)(&server)), nil, (unsafe.Pointer)(s)) {
		return s.Start(id, cluster)
	} else {
		return fmt.Errorf("server already started")
	}
}

func Stop() {
	s := (*Server)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&server))))
	if nil != s {
		s.Stop()
	}
}
