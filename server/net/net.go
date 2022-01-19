package net

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/pkg/crypto"
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

var compressSize = 4096

var defaultKey []byte = []byte("feiyu_tech_2021")

func unpack(key []byte, b []byte) (msg interface{}, err error) {
	if len(b) < 1 {
		err = errors.New("invaild packet")
		return
	}

	var encryptFlag byte
	var cmd uint16
	var context int64
	var compressFlag byte
	var protoBytes []byte
	var l int32
	encryptFlag = b[0]

	if encryptFlag == byte(1) && len(key) == 0 {
		err = errors.New("invaild packet")
		return
	}

	var r buffer.BufferReader
	if encryptFlag == byte(0) {
		r = buffer.NewReader(b[1:])
	} else {
		var plainbyte []byte
		if plainbyte, err = crypto.AESCBCDecrypter(key, b[1:]); nil != err {
			return
		} else {
			r = buffer.NewReader(plainbyte)
		}
	}

	if cmd, err = r.CheckGetUint16(); nil != err {
		return
	}

	if context, err = r.CheckGetInt64(); nil != err {
		return
	}

	if compressFlag, err = r.CheckGetByte(); nil != err {
		return
	}

	if l, err = r.CheckGetInt32(); nil != err {
		return
	}

	if protoBytes, err = r.CheckGetBytes(int(l)); nil != err {
		return
	}

	var data proto.Message

	if compressFlag == byte(0) {
		data, err = pb.GetNamespace("sproto").Unmarshal(uint32(cmd), protoBytes)
		if nil == err {
			msg = &Message{
				Context: context,
				Msg:     data,
			}
		}
	} else {
		de := getDecompressor()
		defer putDecompressor(de)
		var bb []byte
		bb, err = de.Decompress(protoBytes)
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

func Unpack(b []byte) (msg interface{}, err error) {
	return unpack(defaultKey, b)
}

func pack(key []byte, m interface{}) ([]byte, error) {

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
			defer putCompressor(c)
			data, err = c.Compress(data)
			if nil != err {
				return nil, err
			}
			flagCompress = byte(1)
		}

		if len(key) > 0 {
			bb := make([]byte, 0, 15+len(data))
			bb = buffer.AppendUint16(bb, uint16(cmd))
			bb = buffer.AppendInt64(bb, msg.Context)
			bb = buffer.AppendByte(bb, flagCompress)
			bb = buffer.AppendInt32(bb, int32(len(data)))
			if len(data) > 0 {
				bb = buffer.AppendBytes(bb, data)
			}
			if data, err = crypto.AESCBCEncrypt(key, bb); nil != err {
				return nil, err
			}
			b = make([]byte, 0, len(data)+1)
			b = buffer.AppendByte(b, byte(1))
		} else {
			b = make([]byte, 0, 16+len(data))
			b = buffer.AppendByte(b, byte(0))
			b = buffer.AppendUint16(b, uint16(cmd))
			b = buffer.AppendInt64(b, msg.Context)
			b = buffer.AppendByte(b, flagCompress)
			b = buffer.AppendInt32(b, int32(len(data)))
		}

		if len(data) > 0 {
			b = buffer.AppendBytes(b, data)
		}

		return b, nil
	}
}

func Pack(m interface{}) ([]byte, error) {
	return pack(defaultKey, m)
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
	namespace.Register(&sproto.AddSet{}, uint32(sproto.ServerCmdType_AddSet))
	namespace.Register(&sproto.AddSetResp{}, uint32(sproto.ServerCmdType_AddSetResp))

	namespace.Register(&sproto.RemSet{}, uint32(sproto.ServerCmdType_RemSet))
	namespace.Register(&sproto.RemSetResp{}, uint32(sproto.ServerCmdType_RemSetResp))

	namespace.Register(&sproto.AddNode{}, uint32(sproto.ServerCmdType_AddNode))
	namespace.Register(&sproto.AddNodeResp{}, uint32(sproto.ServerCmdType_AddNodeResp))

	namespace.Register(&sproto.AddLearnerStoreToNode{}, uint32(sproto.ServerCmdType_AddLearnerStoreToNode))
	namespace.Register(&sproto.AddLearnerStoreToNodeResp{}, uint32(sproto.ServerCmdType_AddLearnerStoreToNodeResp))

	namespace.Register(&sproto.PromoteLearnerStore{}, uint32(sproto.ServerCmdType_PromoteLearnerStore))
	namespace.Register(&sproto.PromoteLearnerStoreResp{}, uint32(sproto.ServerCmdType_PromoteLearnerStoreResp))

	namespace.Register(&sproto.RemoveNodeStore{}, uint32(sproto.ServerCmdType_RemoveNodeStore))
	namespace.Register(&sproto.RemoveNodeStoreResp{}, uint32(sproto.ServerCmdType_RemoveNodeStoreResp))

	namespace.Register(&sproto.RemNode{}, uint32(sproto.ServerCmdType_RemNode))
	namespace.Register(&sproto.RemNodeResp{}, uint32(sproto.ServerCmdType_RemNodeResp))

	namespace.Register(&sproto.SetMarkClear{}, uint32(sproto.ServerCmdType_SetMarkClear))
	namespace.Register(&sproto.SetMarkClearResp{}, uint32(sproto.ServerCmdType_SetMarkClearResp))

	namespace.Register(&sproto.GetMeta{}, uint32(sproto.ServerCmdType_GetMeta))
	namespace.Register(&sproto.GetMetaResp{}, uint32(sproto.ServerCmdType_GetMetaResp))

	namespace.Register(&sproto.GetSetStatus{}, uint32(sproto.ServerCmdType_GetSetStatus))
	namespace.Register(&sproto.GetSetStatusResp{}, uint32(sproto.ServerCmdType_GetSetStatusResp))

	namespace.Register(&sproto.MetaAddTable{}, uint32(sproto.ServerCmdType_MetaAddTable))
	namespace.Register(&sproto.MetaAddTableResp{}, uint32(sproto.ServerCmdType_MetaAddTableResp))

	namespace.Register(&sproto.MetaAddFields{}, uint32(sproto.ServerCmdType_MetaAddFields))
	namespace.Register(&sproto.MetaAddFieldsResp{}, uint32(sproto.ServerCmdType_MetaAddFieldsResp))

	//flykv <-> pd
	namespace.Register(&sproto.KvnodeBoot{}, uint32(sproto.ServerCmdType_KvnodeBoot))
	namespace.Register(&sproto.KvnodeBootResp{}, uint32(sproto.ServerCmdType_KvnodeBootResp))

	namespace.Register(&sproto.NotifyNodeStoreOp{}, uint32(sproto.ServerCmdType_NotifyNodeStoreOp))
	namespace.Register(&sproto.NodeStoreOpOk{}, uint32(sproto.ServerCmdType_NodeStoreOpOk))

	namespace.Register(&sproto.IsTransInReady{}, uint32(sproto.ServerCmdType_IsTransInReady))
	namespace.Register(&sproto.IsTransInReadyResp{}, uint32(sproto.ServerCmdType_IsTransInReadyResp))

	namespace.Register(&sproto.NotifySlotTransOut{}, uint32(sproto.ServerCmdType_NotifySlotTransOut))
	namespace.Register(&sproto.SlotTransOutOk{}, uint32(sproto.ServerCmdType_SlotTransOutOk))

	namespace.Register(&sproto.NotifySlotTransIn{}, uint32(sproto.ServerCmdType_NotifySlotTransIn))
	namespace.Register(&sproto.SlotTransInOk{}, uint32(sproto.ServerCmdType_SlotTransInOk))

	namespace.Register(&sproto.NotifyUpdateMeta{}, uint32(sproto.ServerCmdType_NotifyUpdateMeta))

	namespace.Register(&sproto.StoreReportStatus{}, uint32(sproto.ServerCmdType_StoreReportStatus))

	namespace.Register(&sproto.TrasnferLeader{}, uint32(sproto.ServerCmdType_TrasnferLeader))

	//flygate <->pd
	namespace.Register(&sproto.QueryRouteInfo{}, uint32(sproto.ServerCmdType_QueryRouteInfo))
	namespace.Register(&sproto.QueryRouteInfoResp{}, uint32(sproto.ServerCmdType_QueryRouteInfoResp))
	namespace.Register(&sproto.FlyGateHeartBeat{}, uint32(sproto.ServerCmdType_FlyGateHeartBeat))
	namespace.Register(&sproto.GetScanTableMeta{}, uint32(sproto.ServerCmdType_GetScanTableMeta))
	namespace.Register(&sproto.GetScanTableMetaResp{}, uint32(sproto.ServerCmdType_GetScanTableMetaResp))

	//client <->pd
	namespace.Register(&sproto.GetFlyGateList{}, uint32(sproto.ServerCmdType_GetFlyGateList))
	namespace.Register(&sproto.GetFlyGateListResp{}, uint32(sproto.ServerCmdType_GetFlyGateListResp))

	namespace.Register(&sproto.ChangeFlyGate{}, uint32(sproto.ServerCmdType_ChangeFlyGate))
	namespace.Register(&sproto.ChangeFlyGateResp{}, uint32(sproto.ServerCmdType_ChangeFlyGateResp))

	//for test
	namespace.Register(&sproto.PacketTest{}, uint32(sproto.ServerCmdType_PacketTest))

}
