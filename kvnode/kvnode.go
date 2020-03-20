package kvnode

import (
	"fmt"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type netCmd struct {
	h    handler
	conn *cliConn
	msg  *codec.Message
}

type KVNode struct {
	storeMgr      *storeMgr
	stoped        int64
	listener      *tcp.Listener
	dispatcher    *dispatcher
	sqlMgr        *sqlMgr
	cmdChan       []chan *netCmd
	poolStopChans []chan interface{}
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func (this *KVNode) pushNetCmd(h handler, conn *cliConn, msg *codec.Message) {
	uniKey := msg.GetHead().UniKey
	i := util.StringHash(uniKey) % len(this.cmdChan)
	this.cmdChan[i] <- &netCmd{
		h:    h,
		conn: conn,
		msg:  msg,
	}
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
			loginReq, err := login.RecvLoginReq(session)
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

			if !login.SendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			session.SetRecvTimeout(protocol.PingTime * 2)
			session.SetSendQueueSize(10000)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetReceiver(codec.NewReceiver(pb.GetNamespace("request"), loginReq.GetCompress()))
			session.SetEncoder(codec.NewEncoder(pb.GetNamespace("response"), loginReq.GetCompress()))

			session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
				this.dispatcher.OnClose(sess, reason)
			})
			this.dispatcher.OnNewClient(session)
			session.Start(func(event *kendynet.Event) {
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

func poolRoutine(kvnode *KVNode, cmdChan chan *netCmd) {
	for {
		v, ok := <-cmdChan
		if !ok {
			return
		}
		v.h(kvnode, v.conn, v.msg)
	}
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

	this.cmdChan = []chan *netCmd{}
	cpuNum := runtime.NumCPU()

	for i := 0; i < cpuNum*2; i++ {
		cmdChan := make(chan *netCmd, 10000/(cpuNum*2))
		this.cmdChan = append(this.cmdChan, cmdChan)
		go poolRoutine(this, cmdChan)
	}

	Infoln("len(cmdChan)", len(this.cmdChan))

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

		for _, v := range this.cmdChan {
			close(v)
		}

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
