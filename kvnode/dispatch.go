package kvnode

import (
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"reflect"
	"sync"
	"sync/atomic"
)

var sessions sync.Map
var clientCount int64

type handler func(*KVNode, *cliConn, *codec.Message)

type dispatcher struct {
	handlers map[string]handler
}

func (this *dispatcher) Register(msg pb.Message, h handler) {
	msgName := reflect.TypeOf(msg).String()
	if nil == h {
		return
	}
	_, ok := this.handlers[msgName]
	if ok {
		return
	}

	this.handlers[msgName] = h
}

func (this *dispatcher) Dispatch(kvnode *KVNode, session kendynet.StreamSession, msg *codec.Message) {
	if nil != msg {
		name := msg.GetName()
		handler, ok := this.handlers[name]
		if ok {
			handler(kvnode, session.GetUserData().(*cliConn), msg)
		}
	}
}

func (this *dispatcher) OnClose(session kendynet.StreamSession, reason string) {
	u := session.GetUserData()
	if nil != u {
		switch u.(type) {
		case *cliConn:
			u.(*cliConn).clear()
		}
	}
	atomic.AddInt64(&clientCount, -1)
	sessions.Delete(session)

	/*u := session.GetUserData()
	if nil != u {
		u.(*scaner).close()
	}
	atomic.AddInt64(&clientCount, -1)
	sessions.Delete(session)*/
}

func (this *dispatcher) OnNewClient(session kendynet.StreamSession) {
	atomic.AddInt64(&clientCount, 1)
	session.SetUserData(
		&cliConn{
			session:  session,
			replyers: map[int64]*replyer{},
		},
	)
	sessions.Store(session, session)
}

func ping(kvnode *KVNode, conn *cliConn, msg *codec.Message) {
	req := msg.GetData().(*proto.PingReq)
	resp := &proto.PingResp{
		Timestamp: req.GetTimestamp(), //pb.Int64(req.GetTimestamp()),
	}
	conn.send(resp)
}
