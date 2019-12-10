package kvnode

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type KVNode struct {
	storeMgr   *storeMgr
	stoped     int64
	listener   *tcp.Listener
	dispatcher *dispatcher
	sqlMgr     *sqlMgr
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

func (this *KVNode) isStoped() bool {
	return atomic.LoadInt64(&this.stoped) == 1
}

func (this *KVNode) startListener() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession) {
		go func() {

			//config := conf.GetConfig()

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
				Ok:       true,                   //proto.Bool(true),
				Compress: loginReq.GetCompress(), //proto.Bool(config.Compress && loginReq.GetCompress()),
			}

			if !sendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			Infoln("on new client")

			//session.SetRecvTimeout(protocol.PingTime * 2)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetReceiver(codec.NewReceiver(pb.GetNamespace("request"), loginReq.GetCompress()))
			session.SetEncoder(codec.NewEncoder(pb.GetNamespace("response"), loginReq.GetCompress()))

			session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
				this.dispatcher.OnClose(sess, reason)
			})
			this.dispatcher.OnNewClient(session)
			session.Start(func(event *kendynet.Event) {
				Infoln("on message")
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					msg := event.Data.(*codec.Message)
					this.dispatcher.Dispatch(this, session, msg.GetCmd(), msg)
				}
			})
		}()
	})
}

func (this *KVNode) Start(id *int, cluster *string) error {

	var err error

	config := conf.GetConfig()

	dbMetaStr, err := loadMetaString()
	if nil != err {
		return err
	}

	dbmeta, err := dbmeta.NewDBMeta(dbMetaStr)

	if nil != err {
		return err
	}

	this.listener, err = tcp.New("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))

	if nil != err {
		return err
	}

	this.sqlMgr, err = newSqlMgr()

	if nil != err {
		return err
	}

	clusterArray := strings.Split(*cluster, ",")

	mutilRaft := newMutilRaft()

	this.storeMgr = newStoreMgr(this, mutilRaft, dbmeta, id, cluster, config.CacheGroupSize)

	go mutilRaft.serveMutilRaft(clusterArray[*id-1])

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

func (this *KVNode) Stop() {

	if atomic.CompareAndSwapInt64(&this.stoped, 0, 1) {
		Infoln("StopServer")
		//关闭监听
		this.listener.Close()

		//关闭现有连接的读端
		sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).ShutdownRead()
			return true
		})

		Infoln("ShutdownRead ok", "wait4ReplyCount:", wait4ReplyCount)

		waitCondition(func() bool {
			if atomic.LoadInt64(&wait4ReplyCount) == 0 {
				return true
			} else {
				return false
			}
		})

		this.sqlMgr.stop()

		Infoln("sql stop ok", "totalUpdateSqlCount:", this.sqlMgr.totalUpdateSqlCount)

		//关闭所有客户连接

		sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).Close("", 1)
			return true
		})

		waitCondition(func() bool {
			if atomic.LoadInt64(&clientCount) == 0 {
				return true
			} else {
				return false
			}
		})

		this.storeMgr.stop()

		Infoln("flyfish stop ok")

	}
}

func (this *KVNode) initHandler() {

	this.dispatcher = &dispatcher{
		handlers: map[uint16]handler{},
	}

	this.dispatcher.Register(uint16(protocol.CmdType_Del), del)
	this.dispatcher.Register(uint16(protocol.CmdType_Get), get)
	this.dispatcher.Register(uint16(protocol.CmdType_Set), set)
	this.dispatcher.Register(uint16(protocol.CmdType_SetNx), setNx)
	this.dispatcher.Register(uint16(protocol.CmdType_CompareAndSet), compareAndSet)
	this.dispatcher.Register(uint16(protocol.CmdType_CompareAndSetNx), compareAndSetNx)
	this.dispatcher.Register(uint16(protocol.CmdType_Ping), ping)
	this.dispatcher.Register(uint16(protocol.CmdType_IncrBy), incrBy)
	this.dispatcher.Register(uint16(protocol.CmdType_DecrBy), decrBy)
	this.dispatcher.Register(uint16(protocol.CmdType_Kick), kick)
	this.dispatcher.Register(uint16(protocol.CmdType_Cancel), cancel)
	this.dispatcher.Register(uint16(protocol.CmdType_ReloadTableConf), reloadTableMeta)

}

func NewKvNode() *KVNode {
	s := &KVNode{}
	s.initHandler()
	return s
}
