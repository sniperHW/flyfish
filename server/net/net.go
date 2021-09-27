package net

import (
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	sproto "github.com/sniperHW/flyfish/server/proto"
)

func Unpack(b []byte) (msg proto.Message, err error) {
	r := buffer.NewReader(b)
	cmd := r.GetUint16()
	msg, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), b[2:])
	return
}

func Pack(msg proto.Message) ([]byte, error) {
	if data, cmd, err := pb.GetNamespace("sproto").Marshal(msg); nil != err {
		return nil, err
	} else {
		b := make([]byte, 0, 2+len(data))
		b = buffer.AppendUint16(b, uint16(cmd))
		if len(data) > 0 {
			b = buffer.AppendBytes(b, data)
		}
		return b, nil
	}
}

func init() {
	namespace := pb.GetNamespace("sproto")

	namespace.Register(&sproto.QueryLeader{}, uint32(sproto.ServerCmdType_QueryLeader))
	namespace.Register(&sproto.QueryLeaderResp{}, uint32(sproto.ServerCmdType_QueryLeaderResp))

	namespace.Register(&sproto.GateReport{}, uint32(sproto.ServerCmdType_GateReport))
	namespace.Register(&sproto.QueryGateList{}, uint32(sproto.ServerCmdType_QueryGateList))
	namespace.Register(&sproto.GateList{}, uint32(sproto.ServerCmdType_GateList))
	namespace.Register(&sproto.NotifyReloadKvconf{}, uint32(sproto.ServerCmdType_NotiReloadKvConf))
	namespace.Register(&sproto.RemoveGate{}, uint32(sproto.ServerCmdType_RemoveGate))

	//console <-> pd
	namespace.Register(&sproto.InstallDeployment{}, uint32(sproto.ServerCmdType_InstallDeployment))
	namespace.Register(&sproto.InstallDeploymentResp{}, uint32(sproto.ServerCmdType_InstallDeploymentResp))

	namespace.Register(&sproto.AddSet{}, uint32(sproto.ServerCmdType_AddSet))
	namespace.Register(&sproto.AddSetResp{}, uint32(sproto.ServerCmdType_AddSetResp))

	namespace.Register(&sproto.RemSet{}, uint32(sproto.ServerCmdType_RemSet))
	namespace.Register(&sproto.RemSetResp{}, uint32(sproto.ServerCmdType_RemSetResp))

	namespace.Register(&sproto.AddNode{}, uint32(sproto.ServerCmdType_AddNode))
	namespace.Register(&sproto.AddNodeResp{}, uint32(sproto.ServerCmdType_AddNodeResp))

	namespace.Register(&sproto.NotifyAddNode{}, uint32(sproto.ServerCmdType_NotifyAddNode))
	namespace.Register(&sproto.NotifyAddNodeResp{}, uint32(sproto.ServerCmdType_NotifyAddNodeResp))

	namespace.Register(&sproto.RemNode{}, uint32(sproto.ServerCmdType_RemNode))
	namespace.Register(&sproto.RemNodeResp{}, uint32(sproto.ServerCmdType_RemNodeResp))

	namespace.Register(&sproto.NotifyRemNode{}, uint32(sproto.ServerCmdType_NotifyRemNode))
	namespace.Register(&sproto.NotifyRemNodeResp{}, uint32(sproto.ServerCmdType_NotifyRemNodeResp))

	namespace.Register(&sproto.NotifySlotTransOut{}, uint32(sproto.ServerCmdType_NotifySlotTransOut))
	namespace.Register(&sproto.NotifySlotTransOutResp{}, uint32(sproto.ServerCmdType_NotifySlotTransOutResp))

	namespace.Register(&sproto.NotifySlotTransIn{}, uint32(sproto.ServerCmdType_NotifySlotTransIn))
	namespace.Register(&sproto.NotifySlotTransInResp{}, uint32(sproto.ServerCmdType_NotifySlotTransInResp))

}
