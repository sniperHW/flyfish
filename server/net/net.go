package net

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/compress"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"sync"
	"time"
)

var compressPool = &sync.Pool{
	New: func() interface{} {
		return &compress.ZipCompressor{}
	},
}

func getCompressor() compress.CompressorI {
	return compressPool.Get().(compress.CompressorI)
}

func putCompressor(c compress.CompressorI) {
	compressPool.Put(c)
}

var uncompressPool = &sync.Pool{
	New: func() interface{} {
		return &compress.ZipUnCompressor{}
	},
}

func getUnCompressor() compress.UnCompressorI {
	return uncompressPool.Get().(compress.UnCompressorI)
}

func putUnCompressor(c compress.UnCompressorI) {
	uncompressPool.Put(c)
}

func Unpack(b []byte) (msg proto.Message, err error) {
	r := buffer.NewReader(b)
	var cmd uint16
	cmd, err = r.CheckGetUint16()
	if nil != err {
		return
	}

	var c byte
	c, err = r.CheckGetByte()
	if nil != err {
		return
	}
	var l int32
	l, err = r.CheckGetInt32()
	if nil != err {
		return
	}

	if len(b[7:]) < int(l) {
		err = fmt.Errorf("not enough data for unpack")
		return
	}

	if c == byte(0) {
		msg, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), b[7:])
	} else {
		un := getUnCompressor()
		var bb []byte
		bb, err = un.UnCompress(b[7:])
		if nil != err {
			return
		}
		msg, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), bb)
	}
	return
}

var compressSize = 4096

func Pack(msg proto.Message) ([]byte, error) {
	var data []byte
	var cmd uint32
	var err error
	if data, cmd, err = pb.GetNamespace("sproto").Marshal(msg); nil != err {
		return nil, err
	} else {
		var b []byte
		var flagCompress byte
		if len(data) >= compressSize {
			c := getCompressor()
			data, err = c.Compress(data)
			if nil != err {
				return nil, err
			}
			flagCompress = byte(1)
		}

		b = make([]byte, 0, 7+len(data))
		b = buffer.AppendUint16(b, uint16(cmd))
		b = buffer.AppendByte(b, flagCompress)
		b = buffer.AppendInt32(b, int32(len(data)))
		if len(data) > 0 {
			b = buffer.AppendBytes(b, data)
		}
		return b, nil
	}
}

func UdpCall(remotes []string, req proto.Message, onResp func(chan interface{}, proto.Message)) (ret interface{}) {
	respCh := make(chan interface{})
	uu := make([]*flynet.Udp, len(remotes))
	for k, v := range remotes {
		go func(i int, addr string) {
			u, err := flynet.NewUdp(fmt.Sprintf(":0"), Pack, Unpack)
			if nil == err {
				remoteAddr, err := net.ResolveUDPAddr("udp", addr)
				if nil != err {
					return
				}
				u.SendTo(remoteAddr, req)
				uu[i] = u
				_, r, err := u.ReadFrom(make([]byte, 65535))
				if nil == err {
					onResp(respCh, r.(proto.Message))
				}
			}
		}(k, v)
	}

	ticker := time.NewTicker(3 * time.Second)

	select {
	case ret = <-respCh:
	case <-ticker.C:
	}
	ticker.Stop()

	for _, v := range uu {
		if nil != v {
			v.Close()
		}
	}

	return
}

func init() {
	namespace := pb.GetNamespace("sproto")

	//flykv <-> flygate
	namespace.Register(&sproto.QueryLeader{}, uint32(sproto.ServerCmdType_QueryLeader))
	namespace.Register(&sproto.QueryLeaderResp{}, uint32(sproto.ServerCmdType_QueryLeaderResp))

	//console <-> pd
	namespace.Register(&sproto.InstallDeployment{}, uint32(sproto.ServerCmdType_InstallDeployment))
	namespace.Register(&sproto.InstallDeploymentResp{}, uint32(sproto.ServerCmdType_InstallDeploymentResp))

	namespace.Register(&sproto.AddSet{}, uint32(sproto.ServerCmdType_AddSet))
	namespace.Register(&sproto.AddSetResp{}, uint32(sproto.ServerCmdType_AddSetResp))

	namespace.Register(&sproto.RemSet{}, uint32(sproto.ServerCmdType_RemSet))
	namespace.Register(&sproto.RemSetResp{}, uint32(sproto.ServerCmdType_RemSetResp))

	namespace.Register(&sproto.AddNode{}, uint32(sproto.ServerCmdType_AddNode))
	namespace.Register(&sproto.AddNodeResp{}, uint32(sproto.ServerCmdType_AddNodeResp))

	namespace.Register(&sproto.SetMarkClear{}, uint32(sproto.ServerCmdType_SetMarkClear))
	namespace.Register(&sproto.SetMarkClearResp{}, uint32(sproto.ServerCmdType_SetMarkClearResp))

	//flykv <-> pd
	namespace.Register(&sproto.KvnodeBoot{}, uint32(sproto.ServerCmdType_KvnodeBoot))
	namespace.Register(&sproto.KvnodeBootResp{}, uint32(sproto.ServerCmdType_KvnodeBootResp))

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

	//flygate <->pd
	namespace.Register(&sproto.QueryRouteInfo{}, uint32(sproto.ServerCmdType_QueryRouteInfo))
	namespace.Register(&sproto.QueryRouteInfoResp{}, uint32(sproto.ServerCmdType_QueryRouteInfoResp))

	//client <->pd
	namespace.Register(&sproto.GetFlyGate{}, uint32(sproto.ServerCmdType_GetFlyGate))
	namespace.Register(&sproto.GetFlyGateResp{}, uint32(sproto.ServerCmdType_GetFlyGateResp))

}
