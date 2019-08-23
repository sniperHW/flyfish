package server

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
var clientCount int32

type handler func(kendynet.StreamSession, *codec.Message)

type dispatcher struct {
	handlers map[string]handler
}

var dispatcher_ *dispatcher = &dispatcher{
	handlers: map[string]handler{},
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

func (this *dispatcher) Dispatch(session kendynet.StreamSession, msg *codec.Message) {
	if nil != msg {
		name := msg.GetName()
		handler, ok := this.handlers[name]
		if ok {
			handler(session, msg)
		}
	}
}

func (this *dispatcher) OnClose(session kendynet.StreamSession, reason string) {
	u := session.GetUserData()
	if nil != u {
		u.(*scaner).close()
	}
	atomic.AddInt32(&clientCount, -1)
	sessions.Delete(session)
}

func (this *dispatcher) OnNewClient(session kendynet.StreamSession) {
	atomic.AddInt32(&clientCount, 1)
	sessions.Store(session, session)
}

func onClose(session kendynet.StreamSession, reason string) {
	dispatcher_.OnClose(session, reason)
}

func onNewClient(session kendynet.StreamSession) {
	dispatcher_.OnNewClient(session)
}

func register(msg pb.Message, h handler) {
	dispatcher_.Register(msg, h)
}

func dispatch(session kendynet.StreamSession, msg *codec.Message) {
	dispatcher_.Dispatch(session, msg)
}

func ping(session kendynet.StreamSession, msg *codec.Message) {
	req := msg.GetData().(*proto.PingReq)
	resp := &proto.PingResp{
		Timestamp: pb.Int64(req.GetTimestamp()),
	}
	session.Send(resp)
}

func init() {
	register(&proto.DelReq{}, del)
	register(&proto.GetReq{}, get)
	register(&proto.SetReq{}, set)
	register(&proto.SetNxReq{}, setNx)
	register(&proto.CompareAndSetReq{}, compareAndSet)
	register(&proto.CompareAndSetNxReq{}, compareAndSetNx)
	register(&proto.PingReq{}, ping)
	register(&proto.IncrByReq{}, incrBy)
	register(&proto.DecrByReq{}, decrBy)
	register(&proto.ScanReq{}, scan)
	register(&proto.ReloadTableConfReq{}, reloadTableConf)
	register(&proto.ReloadConfigReq{}, reloadConf)
}
