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
	"strings"
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

func unpack(from *net.UDPAddr, key []byte, b []byte) (msg interface{}, err error) {
	if len(b) < 2 {
		err = errors.New("invaild packet")
		return
	}

	var encryptFlag byte = b[0]
	var compressFlag byte = b[1]

	if encryptFlag == byte(1) && len(key) == 0 {
		err = errors.New("invaild packet1")
		return
	}

	var plainbyte []byte

	if encryptFlag == byte(1) {
		if plainbyte, err = crypto.AESCBCDecrypter(key, b[2:]); nil != err {
			return
		}
	} else {
		plainbyte = b[2:]
	}

	if compressFlag == byte(1) {
		de := getDecompressor()
		defer putDecompressor(de)
		plainbyte, err = de.Decompress(plainbyte)
		if nil != err {
			return
		}
	}

	udpmsg := &sproto.UdpMsg{}
	if err = proto.Unmarshal(plainbyte, udpmsg); nil != err {
		return
	}

	//如果发送端没有指定ip地址，地址为[::]所以，不能与from直接比较，只能比较端口
	lIdx1 := strings.LastIndex(udpmsg.Addr, ":")
	fromAddr := from.String()
	lIdx2 := strings.LastIndex(fromAddr, ":")

	if udpmsg.Addr[lIdx1+1:] != fromAddr[lIdx2+1:] {
		fmt.Println(udpmsg.Addr[lIdx1+1:], fromAddr[lIdx2+1:])
		err = errors.New("invaild packet2")
		return
	}

	var pbmsg proto.Message

	if pbmsg, err = pb.GetNamespace("sproto").Unmarshal(uint32(udpmsg.Cmd), udpmsg.Data); nil == err {
		msg = &Message{
			Context: udpmsg.Context,
			Msg:     pbmsg,
		}
	}
	return
}

func Unpack(from *net.UDPAddr, b []byte) (msg interface{}, err error) {
	return unpack(from, defaultKey, b)
}

func pack(conn *net.UDPConn, key []byte, m interface{}) ([]byte, error) {
	msg := &sproto.UdpMsg{}
	if _, ok := m.(*Message); ok {
		msg.Context = m.(*Message).Context
		msg.Addr = conn.LocalAddr().String()
	} else {
		return nil, errors.New("invaild msg type")
	}

	var data []byte
	var cmd uint32
	var err error
	if data, cmd, err = pb.GetNamespace("sproto").Marshal(m.(*Message).Msg); nil != err {
		return nil, err
	} else {
		msg.Data = data
		msg.Cmd = int32(cmd)
		bMsg, err := proto.Marshal(msg)
		if nil != err {
			return nil, err
		}

		var flagCompress byte
		if len(data) >= compressSize {
			flagCompress = byte(1)
			c := getCompressor()
			defer putCompressor(c)
			bMsg, err = c.Compress(bMsg)
			if nil != err {
				return nil, err
			}
		}

		var encryptFlag byte

		if len(key) > 0 {
			encryptFlag = byte(1)
			if bMsg, err = crypto.AESCBCEncrypt(key, bMsg); nil != err {
				return nil, err
			}
		}

		b := make([]byte, 0, 2+len(bMsg))
		b = buffer.AppendByte(b, encryptFlag)
		b = buffer.AppendByte(b, flagCompress)
		b = buffer.AppendBytes(b, bMsg)
		return b, nil
	}
}

func Pack(conn *net.UDPConn, m interface{}) ([]byte, error) {
	return pack(conn, defaultKey, m)
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
				u.SendTo(addr, req)
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

	namespace.Register(&sproto.MetaRemoveTable{}, uint32(sproto.ServerCmdType_MetaRemoveTable))
	namespace.Register(&sproto.MetaRemoveTableResp{}, uint32(sproto.ServerCmdType_MetaRemoveTableResp))

	namespace.Register(&sproto.MetaRemoveFields{}, uint32(sproto.ServerCmdType_MetaRemoveFields))
	namespace.Register(&sproto.MetaRemoveFieldsResp{}, uint32(sproto.ServerCmdType_MetaRemoveFieldsResp))

	namespace.Register(&sproto.QueryPdLeader{}, uint32(sproto.ServerCmdType_QueryPdLeader))
	namespace.Register(&sproto.QueryPdLeaderResp{}, uint32(sproto.ServerCmdType_QueryPdLeaderResp))

	namespace.Register(&sproto.AddPdNode{}, uint32(sproto.ServerCmdType_AddPdNode))
	namespace.Register(&sproto.AddPdNodeResp{}, uint32(sproto.ServerCmdType_AddPdNodeResp))

	namespace.Register(&sproto.RemovePdNode{}, uint32(sproto.ServerCmdType_RemovePdNode))
	namespace.Register(&sproto.RemovePdNodeResp{}, uint32(sproto.ServerCmdType_RemovePdNodeResp))

	namespace.Register(&sproto.ListPdMembers{}, uint32(sproto.ServerCmdType_ListPdMembers))
	namespace.Register(&sproto.ListPdMembersResp{}, uint32(sproto.ServerCmdType_ListPdMembersResp))

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
