package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type netCmd struct {
	h    handler
	conn *cliConn
	msg  *net.Message
}

type KVNode struct {
	storeMgr      *storeMgr
	stoped        int32
	listener      *net.Listener
	dispatcher    *dispatcher
	sqlMgr        *sqlMgr
	cmdChan       []chan *netCmd
	poolStopChans []chan interface{}
	//
	sessions        sync.Map
	clientCount     int64
	wait4ReplyCount int64
	id              int
	mutilRaft       *mutilRaft
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func (this *KVNode) pushNetCmd(h handler, conn *cliConn, msg *net.Message) {
	uniKey := msg.GetHead().UniKey
	i := util.StringHash(uniKey) % len(this.cmdChan)
	this.cmdChan[i] <- &netCmd{
		h:    h,
		conn: conn,
		msg:  msg,
	}
}

func (this *KVNode) isStoped() bool {
	return atomic.LoadInt32(&this.stoped) == 1
}

/*
	session.SetInBoundProcessor(NewReceiver())
	session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))
	session.BeginRecv(func(s kendynet.StreamSession, msg interface{}) {
		seqno := atomic.AddInt64(&this.seqno, 1)
		processor := this.processors[seqno%int64(len(this.processors))]
		processor.onReq(seqno, s, msg.(*kendynet.ByteBuffer))
	})
*/

func (this *KVNode) startListener() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession, compress bool) {
		go func() {
			session.SetRecvTimeout(protocol.PingTime * 2)
			session.SetSendQueueSize(10000)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetInBoundProcessor(net.NewReceiver(pb.GetNamespace("request"), compress))
			session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))

			session.SetCloseCallBack(func(sess kendynet.StreamSession, reason error) {
				this.dispatcher.OnClose(sess)
			})
			this.dispatcher.OnNewClient(session)

			session.BeginRecv(func(s kendynet.StreamSession, m interface{}) {
				msg := m.(*net.Message)
				this.dispatcher.Dispatch(session, msg.GetCmd(), msg)
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

	this.id = *id

	this.listener, err = net.NewListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort), verifyLogin)

	if nil != err {
		return err
	}

	this.sqlMgr, err = newSqlMgr()

	if nil != err {
		return err
	}

	clusterArray := strings.Split(*cluster, ",")

	peers := map[int]string{}

	var selfUrl string

	for _, v := range clusterArray {
		t := strings.Split(v, "@")
		if len(t) != 2 {
			panic("invaild peer")
		}
		i, err := strconv.Atoi(t[0])
		if nil != err {
			panic(err)
		}
		peers[i] = t[1]
		if i == *id {
			selfUrl = t[1]
		}
	}

	this.storeMgr = newStoreMgr(this, this.mutilRaft, dbmeta, id, peers, config.CacheGroupSize)

	go this.mutilRaft.serveMutilRaft(selfUrl)

	this.cmdChan = []chan *netCmd{}
	cpuNum := runtime.NumCPU()

	for i := 0; i < cpuNum*2; i++ {
		cmdChan := make(chan *netCmd, 10000/(cpuNum*2))
		this.cmdChan = append(this.cmdChan, cmdChan)
		go poolRoutine(this, cmdChan)
	}

	go func() {
		err := this.startListener()
		if nil != err {
			logger.Errorf("server.Start() error:%s\n", err.Error())
		}
		logger.Infof("flyfish listener stop\n")
	}()

	logger.Infof("flyfish start:%s:%d\n", config.ServiceHost, config.ServicePort)

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

	if atomic.CompareAndSwapInt32(&this.stoped, 0, 1) {
		logger.Infof("StopServer\n")
		//关闭监听
		this.listener.Close()

		//关闭现有连接的读端
		this.sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).ShutdownRead()
			return true
		})

		logger.Infof("ShutdownRead ok, wait4ReplyCount:%d\n", this.wait4ReplyCount)

		waitCondition(func() bool {
			if atomic.LoadInt64(&this.wait4ReplyCount) == 0 {
				return true
			} else {
				return false
			}
		})

		this.sqlMgr.stop()

		logger.Infof("sql stop ok", "totalUpdateSqlCount:%d\n", this.sqlMgr.totalUpdateSqlCount)

		//关闭所有客户连接

		this.sessions.Range(func(key, value interface{}) bool {
			value.(kendynet.StreamSession).Close(nil, 1)
			return true
		})

		waitCondition(func() bool {
			if atomic.LoadInt64(&this.clientCount) == 0 {
				return true
			} else {
				return false
			}
		})

		for _, v := range this.cmdChan {
			close(v)
		}

		this.storeMgr.stop()
		this.mutilRaft.stop()

		logger.Info("flyfish stop ok")

	}
}

func (this *KVNode) initHandler() {

	this.dispatcher = &dispatcher{
		handlers: map[uint16]handler{},
		kvnode:   this,
	}

	this.dispatcher.Register(uint16(protocol.CmdType_Del), del)
	this.dispatcher.Register(uint16(protocol.CmdType_Get), get)
	this.dispatcher.Register(uint16(protocol.CmdType_Set), set)
	this.dispatcher.Register(uint16(protocol.CmdType_SetNx), setNx)
	this.dispatcher.Register(uint16(protocol.CmdType_CompareAndSet), compareAndSet)
	this.dispatcher.Register(uint16(protocol.CmdType_CompareAndSetNx), compareAndSetNx)
	this.dispatcher.Register(uint16(protocol.CmdType_IncrBy), incrBy)
	this.dispatcher.Register(uint16(protocol.CmdType_DecrBy), decrBy)
	this.dispatcher.Register(uint16(protocol.CmdType_Kick), kick)
	//this.dispatcher.Register(uint16(protocol.CmdType_Cancel), cancel)
	//this.dispatcher.Register(uint16(protocol.CmdType_ReloadTableConf), reloadTableMeta)

}

func NewKvNode() *KVNode {
	s := &KVNode{
		mutilRaft: newMutilRaft(),
	}
	s.initHandler()
	return s
}
