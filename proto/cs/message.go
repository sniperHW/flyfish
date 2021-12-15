package cs

import (
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
)

type ReqMessage struct {
	Seqno   int64
	UniKey  string
	Timeout uint32
	Store   int
	Cmd     protocol.CmdType
	Data    proto.Message
}

type RespMessage struct {
	Seqno int64
	Err   errcode.Error
	Cmd   protocol.CmdType
	Data  proto.Message
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
}
