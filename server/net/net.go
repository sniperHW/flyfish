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

}
