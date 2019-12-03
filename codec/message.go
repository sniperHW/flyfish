package codec

import (
	"github.com/sniperHW/flyfish/codec/pb"
	protocol "github.com/sniperHW/flyfish/proto"

	"github.com/golang/protobuf/proto"
)

type CommonHead struct {
	Seqno   int64
	UniKey  string
	ErrCode int32
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

	pb.Register(&protocol.PingReq{}, uint32(protocol.CmdType_PingReq))
	pb.Register(&protocol.PingResp{}, uint32(protocol.CmdType_PingResp))

	pb.Register(&protocol.SetReq{}, uint32(protocol.CmdType_SetReq))
	pb.Register(&protocol.SetResp{}, uint32(protocol.CmdType_SetResp))

	pb.Register(&protocol.GetReq{}, uint32(protocol.CmdType_GetReq))
	pb.Register(&protocol.GetResp{}, uint32(protocol.CmdType_GetResp))

	pb.Register(&protocol.DelReq{}, uint32(protocol.CmdType_DelReq))
	pb.Register(&protocol.DelResp{}, uint32(protocol.CmdType_DelResp))

	pb.Register(&protocol.IncrByReq{}, uint32(protocol.CmdType_IncrByReq))
	pb.Register(&protocol.IncrByResp{}, uint32(protocol.CmdType_IncrByResp))

	pb.Register(&protocol.DecrByReq{}, uint32(protocol.CmdType_DecrByReq))
	pb.Register(&protocol.DecrByResp{}, uint32(protocol.CmdType_DecrByResp))

	pb.Register(&protocol.SetNxReq{}, uint32(protocol.CmdType_SetNxReq))
	pb.Register(&protocol.SetNxResp{}, uint32(protocol.CmdType_SetNxResp))

	pb.Register(&protocol.CompareAndSetReq{}, uint32(protocol.CmdType_CompareAndSetReq))
	pb.Register(&protocol.CompareAndSetResp{}, uint32(protocol.CmdType_CompareAndSetResp))

	pb.Register(&protocol.CompareAndSetNxReq{}, uint32(protocol.CmdType_CompareAndSetNxReq))
	pb.Register(&protocol.CompareAndSetNxResp{}, uint32(protocol.CmdType_CompareAndSetNxResp))

	pb.Register(&protocol.CompareAndSetNxReq{}, uint32(protocol.CmdType_CompareAndSetNxReq))
	pb.Register(&protocol.CompareAndSetNxResp{}, uint32(protocol.CmdType_CompareAndSetNxResp))

	pb.Register(&protocol.KickReq{}, uint32(protocol.CmdType_KickReq))
	pb.Register(&protocol.KickResp{}, uint32(protocol.CmdType_KickResp))

	//pb.Register(&protocol.ScanReq{}, uint32(protocol.CmdType_ScanReq))
	//pb.Register(&protocol.ScanResp{}, uint32(protocol.CmdType_ScanResp))

	pb.Register(&protocol.ReloadTableConfReq{}, uint32(protocol.CmdType_ReloadTableConfReq))
	pb.Register(&protocol.ReloadTableConfResp{}, uint32(protocol.CmdType_ReloadTableConfResp))

	pb.Register(&protocol.ReloadConfigReq{}, uint32(protocol.CmdType_ReloadConfigReq))
	pb.Register(&protocol.ReloadConfigResp{}, uint32(protocol.CmdType_ReloadConfigResp))

	pb.Register(&protocol.Cancel{}, uint32(protocol.CmdType_CancelReq))

}
