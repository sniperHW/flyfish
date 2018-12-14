package flyfish

import(
	"fmt"
	codec "flyfish/codec"
	"github.com/sniperHW/kendynet"
	"reflect"
	"github.com/golang/protobuf/proto"
	protocol "flyfish/proto"
)

type handler func(kendynet.StreamSession,*codec.Message)

type dispatcher struct {
	handlers map[string]handler
}

var dispatcher_ *dispatcher

func (this *dispatcher) Register(msg proto.Message,h handler) {
	msgName := reflect.TypeOf(msg).String()	
	if nil == h {
		return
	}
	_,ok := this.handlers[msgName]
	if ok {
		return
	}

	this.handlers[msgName] = h
}

func (this *dispatcher) Dispatch(session kendynet.StreamSession,msg *codec.Message) {
	//fmt.Println("Dispatch")
	if nil != msg {
		name := msg.GetName()
		handler,ok := this.handlers[name]
		if ok {
			handler(session,msg)
		}
	}
}

func (this *dispatcher) OnClose(session kendynet.StreamSession,reason string) {
	fmt.Printf("client close:%s\n",reason)
	u := session.GetUserData()
	if nil != u {
		u.(*scaner).close()
	}
}

func (this *dispatcher) OnNewClient(session kendynet.StreamSession) {
	//fmt.Printf("new client\n")	
}

func onClose(session kendynet.StreamSession,reason string) {
	dispatcher_.OnClose(session,reason)
}

func onNewClient(session kendynet.StreamSession) {
	dispatcher_.OnNewClient(session)
}

func register(msg proto.Message,h handler) {
	dispatcher_.Register(msg,h)
}

func dispatch(session kendynet.StreamSession,msg *codec.Message) {
	dispatcher_.Dispatch(session,msg)
}


func ping(session kendynet.StreamSession,msg *codec.Message) {
	req  := msg.GetData().(*protocol.PingReq)
	resp := &protocol.PingResp{
		Timestamp : proto.Int64(req.GetTimestamp()),
	}
	session.Send(resp)
}

func init() {
	dispatcher_ = &dispatcher {
		handlers : map[string]handler{},
	}

	register(&protocol.DelReq{},del)
	register(&protocol.GetReq{},get)
	register(&protocol.GetAllReq{},getAll)
	register(&protocol.SetReq{},set)
	register(&protocol.SetNxReq{},setNx)
	register(&protocol.CompareAndSetReq{},compareAndSet)
	register(&protocol.CompareAndSetNxReq{},compareAndSetNx)
	register(&protocol.PingReq{},ping)
	register(&protocol.IncrByReq{},incrBy)
	register(&protocol.DecrByReq{},decrBy)
	register(&protocol.ScanReq{},scan)
}

