package net

import (
	"errors"
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

var decompressPool = &sync.Pool{
	New: func() interface{} {
		return &compress.ZipDecompressor{}
	},
}

func getDecompressor() compress.DecompressorI {
	return decompressPool.Get().(compress.DecompressorI)
}

func putDecompressor(c compress.DecompressorI) {
	decompressPool.Put(c)
}

type Message struct {
	Context int64
	Msg     proto.Message
}

func MakeMessage(context int64, msg proto.Message) *Message {
	return &Message{
		Context: context,
		Msg:     msg,
	}
}

func Unpack(b []byte) (msg interface{}, err error) {
	r := buffer.NewReader(b)
	var cmd uint16
	cmd, err = r.CheckGetUint16()
	if nil != err {
		return
	}

	var context int64
	context, err = r.CheckGetInt64()
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

	if len(b[15:]) < int(l) {
		err = fmt.Errorf("not enough data for unpack")
		return
	}

	var data proto.Message

	if c == byte(0) {
		data, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), b[15:])
		if nil == err {
			msg = &Message{
				Context: context,
				Msg:     data,
			}
		}
	} else {
		un := getDecompressor()
		var bb []byte
		bb, err = un.Decompress(b[15:])
		if nil != err {
			return
		}
		data, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), bb)
		if nil == err {
			msg = &Message{
				Context: context,
				Msg:     data,
			}
		}
	}
	return
}

var compressSize = 4096

func Pack(m interface{}) ([]byte, error) {

	msg, ok := m.(*Message)

	if !ok {
		return nil, errors.New("invaild msg type")
	}

	var data []byte
	var cmd uint32
	var err error
	if data, cmd, err = pb.GetNamespace("sproto").Marshal(msg.Msg); nil != err {
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

		b = make([]byte, 0, 15+len(data))
		b = buffer.AppendUint16(b, uint16(cmd))
		b = buffer.AppendInt64(b, msg.Context)
		b = buffer.AppendByte(b, flagCompress)
		b = buffer.AppendInt32(b, int32(len(data)))
		if len(data) > 0 {
			b = buffer.AppendBytes(b, data)
		}
		return b, nil
	}
}

func UdpCall(remotes interface{}, req *Message, timeout time.Duration, onResp func(chan interface{}, interface{})) (ret interface{}) {
	var remoteAddrs []*net.UDPAddr
	switch remotes.(type) {
	case []string:
		for _, v := range remotes.([]string) {
			if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
				remoteAddrs = append(remoteAddrs, addr)
			}
		}
	case []*net.UDPAddr:
		remoteAddrs = remotes.([]*net.UDPAddr)
	}

	if len(remoteAddrs) == 0 {
		panic("invaild remotes")
	}

	respCh := make(chan interface{})
	uu := make([]*flynet.Udp, len(remoteAddrs))
	for k, v := range remoteAddrs {
		go func(i int, addr *net.UDPAddr) {
			u, err := flynet.NewUdp(fmt.Sprintf(":0"), Pack, Unpack)
			if nil == err {
				u.SendTo(v, req)
				uu[i] = u
				_, r, err := u.ReadFrom(make([]byte, 65535))
				if nil == err {
					onResp(respCh, r)
				}
			}
		}(k, v)
	}

	ticker := time.NewTicker(timeout)

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

//只要分钟级不重复即可
func MakeUniqueContext() int64 {
	return time.Now().UnixNano()
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

	namespace.Register(&sproto.SetMeta{}, uint32(sproto.ServerCmdType_SetMeta))
	namespace.Register(&sproto.SetMetaResp{}, uint32(sproto.ServerCmdType_SetMetaResp))

	namespace.Register(&sproto.UpdateMeta{}, uint32(sproto.ServerCmdType_UpdateMeta))
	namespace.Register(&sproto.UpdateMetaResp{}, uint32(sproto.ServerCmdType_UpdateMetaResp))

	namespace.Register(&sproto.GetMeta{}, uint32(sproto.ServerCmdType_GetMeta))
	namespace.Register(&sproto.GetMetaResp{}, uint32(sproto.ServerCmdType_GetMetaResp))

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
	namespace.Register(&sproto.FlyGateHeartBeat{}, uint32(sproto.ServerCmdType_FlyGateHeartBeat))

	//client <->pd
	namespace.Register(&sproto.GetFlyGateList{}, uint32(sproto.ServerCmdType_GetFlyGateList))
	namespace.Register(&sproto.GetFlyGateListResp{}, uint32(sproto.ServerCmdType_GetFlyGateListResp))

	namespace.Register(&sproto.ChangeFlyGate{}, uint32(sproto.ServerCmdType_ChangeFlyGate))
	namespace.Register(&sproto.ChangeFlyGateResp{}, uint32(sproto.ServerCmdType_ChangeFlyGateResp))

}
