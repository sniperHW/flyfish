package sqlnode

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"runtime"
	"time"
)

//type message struct {
//	conn *cliConn
//	msg  *net.Message
//}
//
//var (
//	cNetCmd chan *message
//)
//
//func initMessageRoutine() {
//	cNetCmd = make(chan *message, 10000)
//
//	go func() {
//		for m := range cNetCmd {
//			h := messageHandlers[m.msg.GetCmd()]
//			// todo pcall?
//			h(m.conn, m.msg)
//		}
//	}()
//}
//
//func pushMessage(r *message) {
//	cNetCmd <- r
//}

type messageHandler func(*cliConn, *net.Message)

var (
	messageHandlers map[uint16]messageHandler
)

func initMessageHandler() {
	messageHandlers = make(map[uint16]messageHandler)
	registerMessageHandler(uint16(protocol.CmdType_Ping), onPing)
	//registerMessageHandler(uint16(protocol.CmdType_Cancel), onCancel)
	registerMessageHandler(uint16(protocol.CmdType_ReloadTableConf), onReloadTableConf)
	registerMessageHandler(uint16(protocol.CmdType_Get), onGet)
	registerMessageHandler(uint16(protocol.CmdType_Set), onSet)
	registerMessageHandler(uint16(protocol.CmdType_SetNx), onSetNx)
	registerMessageHandler(uint16(protocol.CmdType_CompareAndSet), onCompareSet)
	registerMessageHandler(uint16(protocol.CmdType_CompareAndSetNx), onCompareSetNx)
	registerMessageHandler(uint16(protocol.CmdType_IncrBy), onIncrBy)
	registerMessageHandler(uint16(protocol.CmdType_DecrBy), onDecrBy)
	registerMessageHandler(uint16(protocol.CmdType_Del), onDel)

	getLogger().Infoln("init message handler.")

}

func registerMessageHandler(cmd uint16, h messageHandler) {
	if h == nil {
		panic("nil handler")
	}

	if _, ok := messageHandlers[cmd]; !ok {
		messageHandlers[cmd] = h
	}
}

func dispatchMessage(session kendynet.StreamSession, cmd uint16, msg *net.Message) {
	if h, ok := messageHandlers[cmd]; ok {
		//投递给线程池处理

		defer func() {
			if err := recover(); err != nil {
				buff := make([]byte, 1024)
				n := runtime.Stack(buff, false)
				getLogger().Errorf("dispatch message: %s.\n%s", err, buff[:n])
			}
		}()

		h(session.GetUserData().(*cliConn), msg)

		//pushMessage(&message{
		//	conn: session.GetUserData().(*cliConn),
		//	msg:  msg,
		//})
	} else {
		getLogger().Errorf("message(%d) handler not found.", msg.GetCmd())
	}

	//if nil != msg {
	//
	//	switch cmd {
	//	case uint16(protocol.CmdType_Ping):
	//		session.Send(net.NewMessage(net.CommonHead{}, &protocol.PingResp{
	//			Timestamp: time.Now().UnixNano(),
	//		}))
	//	case uint16(protocol.CmdType_Cancel):
	//		cancel(this.kvnode, session.GetUserData().(*cliConn), msg)
	//	case uint16(protocol.CmdType_ReloadTableConf):
	//		onReloadTableConf(session.GetUserData().(*cliConn), msg)
	//	default:
	//		if _, ok := messageHandlers[cmd]; ok {
	//			//投递给线程池处理
	//			pushMessage(&message{
	//				conn: session.GetUserData().(*cliConn),
	//				msg:  msg,
	//			})
	//		} else {
	//			getLogger().Errorf("message(%d) handler not found.", msg.GetCmd())
	//		}
	//	}
	//}
}

func newMessage(seqNo int64, errCode int32, pb proto.Message) *net.Message {
	return net.NewMessage(
		net.CommonHead{
			Seqno:   seqNo,
			ErrCode: errCode,
		},
		pb,
	)
}

func onPing(cli *cliConn, msg *net.Message) {
	_ = cli.sendMessage(net.NewMessage(net.CommonHead{}, &protocol.PingResp{
		Timestamp: time.Now().UnixNano(),
	}))
}

//func onCancel(cli *cliConn, msg *net.Message) {
//	req := msg.GetData().(*protocol.Cancel)
//	for _, v := range req.GetSeqs() {
//		cli.remCmdBySeqNo(v)
//	}
//}
