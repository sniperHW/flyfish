package codec

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"strings"
)

type CommonHead struct {
	Seqno   int64
	UniKey  string
	ErrCode int32
	Timeout uint32
}

func (this *CommonHead) SplitUniKey() (table string, key string) {
	r := strings.SplitN(this.UniKey, ":", 2)
	if len(r) == 2 {
		table = r[0]
		key = r[1]
	}
	return
}

type Message struct {
	name string
	data proto.Message
	head CommonHead
}

func NewMessage(name string, head CommonHead, data proto.Message) *Message {
	return &Message{name: name, head: head, data: data}
}

func (this *Message) GetData() proto.Message {
	return this.data
}

func (this *Message) GetHead() CommonHead {
	return this.head
}

func (this *Message) GetName() string {
	return this.name
}

func init() {

	requestSpace := pb.GetNamespace("request")

	requestSpace.Register(&protocol.PingReq{}, uint32(protocol.CmdType_Ping))
	requestSpace.Register(&protocol.SetReq{}, uint32(protocol.CmdType_Set))
	requestSpace.Register(&protocol.GetReq{}, uint32(protocol.CmdType_Get))
	requestSpace.Register(&protocol.DelReq{}, uint32(protocol.CmdType_Del))
	requestSpace.Register(&protocol.IncrByReq{}, uint32(protocol.CmdType_IncrBy))
	requestSpace.Register(&protocol.DecrByReq{}, uint32(protocol.CmdType_DecrBy))
	requestSpace.Register(&protocol.SetNxReq{}, uint32(protocol.CmdType_SetNx))
	requestSpace.Register(&protocol.CompareAndSetReq{}, uint32(protocol.CmdType_CompareAndSet))
	requestSpace.Register(&protocol.CompareAndSetNxReq{}, uint32(protocol.CmdType_CompareAndSetNx))
	requestSpace.Register(&protocol.KickReq{}, uint32(protocol.CmdType_Kick))
	requestSpace.Register(&protocol.ReloadTableConfReq{}, uint32(protocol.CmdType_ReloadTableConf))
	requestSpace.Register(&protocol.Cancel{}, uint32(protocol.CmdType_Cancel))

	responseSpace := pb.GetNamespace("response")

	responseSpace.Register(&protocol.PingResp{}, uint32(protocol.CmdType_Ping))
	responseSpace.Register(&protocol.SetResp{}, uint32(protocol.CmdType_Set))
	responseSpace.Register(&protocol.GetResp{}, uint32(protocol.CmdType_Get))
	responseSpace.Register(&protocol.DelResp{}, uint32(protocol.CmdType_Del))
	responseSpace.Register(&protocol.IncrByResp{}, uint32(protocol.CmdType_IncrBy))
	responseSpace.Register(&protocol.DecrByResp{}, uint32(protocol.CmdType_DecrBy))
	responseSpace.Register(&protocol.SetNxResp{}, uint32(protocol.CmdType_SetNx))
	responseSpace.Register(&protocol.CompareAndSetResp{}, uint32(protocol.CmdType_CompareAndSet))
	responseSpace.Register(&protocol.CompareAndSetNxResp{}, uint32(protocol.CmdType_CompareAndSetNx))
	responseSpace.Register(&protocol.CompareAndSetNxResp{}, uint32(protocol.CmdType_CompareAndSetNx))
	responseSpace.Register(&protocol.KickResp{}, uint32(protocol.CmdType_Kick))
	responseSpace.Register(&protocol.ReloadTableConfResp{}, uint32(protocol.CmdType_ReloadTableConf))

}
