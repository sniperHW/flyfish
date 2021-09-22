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

	namespace.Register(&sproto.KvnodeBoot{}, uint32(sproto.ServerCmdType_KvnodeBoot))
	namespace.Register(&sproto.KvnodeBootResp{}, uint32(sproto.ServerCmdType_KvnodeBootResp))

	namespace.Register(&sproto.AddKvnode{}, uint32(sproto.ServerCmdType_AddKvnode))
	namespace.Register(&sproto.AddKvnodeResp{}, uint32(sproto.ServerCmdType_AddKvnodeResp))

	namespace.Register(&sproto.RemKvnode{}, uint32(sproto.ServerCmdType_RemKvnode))
	namespace.Register(&sproto.RemKvnodeResp{}, uint32(sproto.ServerCmdType_RemKvnodeResp))

	namespace.Register(&sproto.AddStore{}, uint32(sproto.ServerCmdType_AddStore))
	namespace.Register(&sproto.AddStoreResp{}, uint32(sproto.ServerCmdType_AddStoreResp))

	namespace.Register(&sproto.RemStore{}, uint32(sproto.ServerCmdType_RemStore))
	namespace.Register(&sproto.RemStoreResp{}, uint32(sproto.ServerCmdType_RemStoreResp))

	namespace.Register(&sproto.KvnodeAddStore{}, uint32(sproto.ServerCmdType_KvnodeAddStore))
	namespace.Register(&sproto.KvnodeAddStoreResp{}, uint32(sproto.ServerCmdType_KvnodeAddStoreResp))

	namespace.Register(&sproto.KvnodeRemStore{}, uint32(sproto.ServerCmdType_KvnodeRemStore))
	namespace.Register(&sproto.KvnodeRemStoreResp{}, uint32(sproto.ServerCmdType_KvnodeRemStoreResp))

	namespace.Register(&sproto.NotifyKvnodeStoreTrans{}, uint32(sproto.ServerCmdType_NotifyKvnodeStoreTrans))
	namespace.Register(&sproto.NotifyKvnodeStoreTransResp{}, uint32(sproto.ServerCmdType_NotifyKvnodeStoreTransResp))

	namespace.Register(&sproto.SlotTransferPrepare{}, uint32(sproto.ServerCmdType_SlotTransferPrepare))
	namespace.Register(&sproto.SlotTransferPrepareAck{}, uint32(sproto.ServerCmdType_SlotTransferPrepareAck))

	namespace.Register(&sproto.SlotTransferCancel{}, uint32(sproto.ServerCmdType_SlotTransferCancel))
	namespace.Register(&sproto.SlotTransferCommit{}, uint32(sproto.ServerCmdType_SlotTransferCommit))

}
