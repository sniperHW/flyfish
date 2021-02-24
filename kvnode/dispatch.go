package kvnode

import (
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"sync/atomic"
	"time"
)

type handler func(*KVNode, *cliConn, *net.Message)

type dispatcher struct {
	handlers map[uint16]handler
	kvnode   *KVNode
}

func (this *dispatcher) Register(cmd uint16, h handler) {
	if _, ok := this.handlers[cmd]; !ok {
		this.handlers[cmd] = h
	}
}

func (this *dispatcher) Dispatch(session kendynet.StreamSession, cmd uint16, msg *net.Message) {
	if nil != msg {
		switch cmd {
		case uint16(proto.CmdType_Ping):
			session.Send(net.NewMessage(net.CommonHead{}, &proto.PingResp{
				Timestamp: time.Now().UnixNano(),
			}))
		case uint16(proto.CmdType_Cancel):
			cancel(this.kvnode, session.GetUserData().(*cliConn), msg)
		case uint16(proto.CmdType_ReloadTableConf):
			reloadTableMeta(this.kvnode, session.GetUserData().(*cliConn), msg)
		default:
			if handler, ok := this.handlers[cmd]; ok {
				//投递给线程池处理
				this.kvnode.pushNetCmd(handler, session.GetUserData().(*cliConn), msg)
			}
		}
	}
}

func (this *dispatcher) OnClose(session kendynet.StreamSession) {
	if u := session.GetUserData(); nil != u {
		switch u.(type) {
		case *cliConn:
			u.(*cliConn).clear()
		}
	}
	atomic.AddInt64(&this.kvnode.clientCount, -1)
	this.kvnode.sessions.Delete(session)
}

func (this *dispatcher) OnNewClient(session kendynet.StreamSession) {
	atomic.AddInt64(&this.kvnode.clientCount, 1)
	session.SetUserData(
		&cliConn{
			session:  session,
			replyers: map[int64]*replyer{},
			node:     this.kvnode,
		},
	)
	this.kvnode.sessions.Store(session, session)
}
