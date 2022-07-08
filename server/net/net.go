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
	"reflect"
	"sync"
	"time"
)

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
		de := compress.GetGZipDecompressor()
		defer compress.PutGZipDecompressor(de)
		plainbyte, err = de.Decompress(plainbyte)
		if nil != err {
			return
		}
	}

	udpmsg := &sproto.UdpMsg{}
	if err = proto.Unmarshal(plainbyte, udpmsg); nil != err {
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
			c := compress.GetGZipCompressor()
			defer compress.PutGZipCompressor(c)
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

var udpPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		u, err := flynet.NewUdp(fmt.Sprintf(":0"), Pack, Unpack)
		return []interface{}{u, err}
	},
}

func getUdp() (*flynet.Udp, error) {
	v := udpPool.Get()
	switch v.(type) {
	case *flynet.Udp:
		return v.(*flynet.Udp), nil
	case []interface{}:
		if v.([]interface{})[0] != nil {
			return v.([]interface{})[0].(*flynet.Udp), nil
		} else {
			return nil, v.([]interface{})[1].(error)
		}
	}
	return nil, errors.New("unknown error")
}

func isNetTimeout(err error) bool {
	switch err.(type) {
	case net.Error:
		if err.(net.Error).Timeout() {
			return true
		}
	default:
	}
	return false
}

func UdpCall(remotes interface{}, req proto.Message, respType proto.Message, timeout time.Duration) (resp proto.Message, err error) {
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

	udpConn, err := getUdp()
	if nil != err {
		return nil, err
	}

	contexts := map[int64]bool{}
	for _, v := range remoteAddrs {
		context := MakeUniqueContext()
		contexts[context] = true
		udpConn.SendTo(v, MakeMessage(context, req))
	}

	udpConn.SetReadDeadline(time.Now().Add(timeout))
	recvbuff := make([]byte, 65535)
	for {
		if _, r, err := udpConn.ReadFrom(recvbuff); nil == err {
			if m, ok := r.(*Message); ok && contexts[m.Context] {
				if reflect.TypeOf(m.Msg) == reflect.TypeOf(respType) {
					resp = m.Msg
					udpPool.Put(udpConn)
					return resp, nil
				}
			}
		} else {
			if isNetTimeout(err) {
				udpPool.Put(udpConn)
			} else {
				udpConn.Close()
			}
			return resp, err
		}
	}
}

//只要分钟级不重复即可
func MakeUniqueContext() int64 {
	return time.Now().UnixNano()
}

const (
	SizeCTX       = 8
	SizeLen       = 4
	SizeCmd       = 2
	MinSize       = SizeLen
	MaxPacketSize = 8 * 1024 * 1024
)

type Encoder struct {
}

func (this *Encoder) EnCode(o interface{}, buff *buffer.Buffer) error {

	m, ok := o.(*Message)

	if !ok {
		if nil == o {
			panic("o is nil")
		}
		return fmt.Errorf("invaild object to encode:%s", reflect.TypeOf(o).String())
	}

	if nil == m.Msg {
		return errors.New("Msg is nil")
	}

	if pbbytes, cmd, err := pb.GetNamespace("sproto").Marshal(m.Msg); err != nil {
		return err
	} else {

		payloadLen := SizeCTX + SizeCmd + len(pbbytes)
		totalLen := SizeLen + payloadLen
		if uint64(totalLen) > MaxPacketSize {
			return fmt.Errorf("packet too large")
		}

		//写payload大小
		buff.AppendUint32(uint32(payloadLen))
		//cmd
		buff.AppendUint16(uint16(cmd))
		//seqno
		buff.AppendInt64(m.Context)
		buff.AppendBytes(pbbytes)

		return nil
	}
}

func inbouncUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= MinSize {
		var msg proto.Message

		reader := buffer.NewReader(b[r : r+unpackSize])
		payload := int(reader.GetUint32())

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+SizeLen > MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+SizeLen)
			return
		}

		totalSize := payload + SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			m := &Message{}
			m.Context = reader.GetInt64()
			cmd := reader.GetUint16()
			pbsize := payload - SizeCTX - SizeCmd
			buff := reader.GetBytes(pbsize)
			if msg, err = pbSpace.Unmarshal(uint32(cmd), buff); err != nil {
				return
			} else {
				m.Msg = msg
				ret = m
			}
		}
	}
	return
}

func NewReqInboundProcessor() *flynet.InboundProcessor {
	return flynet.NewInboundProcessor(inbouncUnpack, pb.GetNamespace("sproto"))
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

	namespace.Register(&sproto.RemNode{}, uint32(sproto.ServerCmdType_RemNode))
	namespace.Register(&sproto.RemNodeResp{}, uint32(sproto.ServerCmdType_RemNodeResp))

	namespace.Register(&sproto.SetMarkClear{}, uint32(sproto.ServerCmdType_SetMarkClear))
	namespace.Register(&sproto.SetMarkClearResp{}, uint32(sproto.ServerCmdType_SetMarkClearResp))

	namespace.Register(&sproto.GetMeta{}, uint32(sproto.ServerCmdType_GetMeta))
	namespace.Register(&sproto.GetMetaResp{}, uint32(sproto.ServerCmdType_GetMetaResp))

	namespace.Register(&sproto.GetKvStatus{}, uint32(sproto.ServerCmdType_GetKvStatus))
	namespace.Register(&sproto.GetKvStatusResp{}, uint32(sproto.ServerCmdType_GetKvStatusResp))

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

	namespace.Register(&sproto.ClearDBData{}, uint32(sproto.ServerCmdType_ClearDBData))
	namespace.Register(&sproto.ClearDBDataResp{}, uint32(sproto.ServerCmdType_ClearDBDataResp))

	namespace.Register(&sproto.ClearCache{}, uint32(sproto.ServerCmdType_ClearCache))
	namespace.Register(&sproto.ClearCacheResp{}, uint32(sproto.ServerCmdType_ClearCacheResp))

	namespace.Register(&sproto.SuspendKvStore{}, uint32(sproto.ServerCmdType_SuspendKvStore))
	namespace.Register(&sproto.SuspendKvStoreResp{}, uint32(sproto.ServerCmdType_SuspendKvStoreResp))

	namespace.Register(&sproto.ResumeKvStore{}, uint32(sproto.ServerCmdType_ResumeKvStore))
	namespace.Register(&sproto.ResumeKvStoreResp{}, uint32(sproto.ServerCmdType_ResumeKvStoreResp))

	namespace.Register(&sproto.GetDeployment{}, uint32(sproto.ServerCmdType_GetDeployment))
	namespace.Register(&sproto.GetDeploymentResp{}, uint32(sproto.ServerCmdType_GetDeploymentResp))

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

	namespace.Register(&sproto.KvnodeReportStatus{}, uint32(sproto.ServerCmdType_KvnodeReportStatus))

	namespace.Register(&sproto.NotifyMissingStores{}, uint32(sproto.ServerCmdType_NotifyMissingStores))

	namespace.Register(&sproto.TrasnferLeader{}, uint32(sproto.ServerCmdType_TrasnferLeader))

	namespace.Register(&sproto.ClearStoreCache{}, uint32(sproto.ServerCmdType_ClearStoreCache))

	namespace.Register(&sproto.SuspendStore{}, uint32(sproto.ServerCmdType_SuspendStore))

	namespace.Register(&sproto.ResumeStore{}, uint32(sproto.ServerCmdType_ResumeStore))

	//flygate <->pd
	namespace.Register(&sproto.QueryRouteInfo{}, uint32(sproto.ServerCmdType_QueryRouteInfo))
	namespace.Register(&sproto.QueryRouteInfoResp{}, uint32(sproto.ServerCmdType_QueryRouteInfoResp))
	namespace.Register(&sproto.FlyGateHeartBeat{}, uint32(sproto.ServerCmdType_FlyGateHeartBeat))
	namespace.Register(&sproto.GetScanTableMeta{}, uint32(sproto.ServerCmdType_GetScanTableMeta))
	namespace.Register(&sproto.GetScanTableMetaResp{}, uint32(sproto.ServerCmdType_GetScanTableMetaResp))

	//flysql <-> pd
	namespace.Register(&sproto.FlySqlHeartBeat{}, uint32(sproto.ServerCmdType_FlySqlHeartBeat))
	namespace.Register(&sproto.FlySqlHeartBeatResp{}, uint32(sproto.ServerCmdType_FlySqlHeartBeatResp))

	//client <->pd
	namespace.Register(&sproto.GetFlyGateList{}, uint32(sproto.ServerCmdType_GetFlyGateList))
	namespace.Register(&sproto.GetFlyGateListResp{}, uint32(sproto.ServerCmdType_GetFlyGateListResp))

	namespace.Register(&sproto.GetFlySqlList{}, uint32(sproto.ServerCmdType_GetFlySqlList))
	namespace.Register(&sproto.GetFlySqlListResp{}, uint32(sproto.ServerCmdType_GetFlySqlListResp))

	namespace.Register(&sproto.OrderSequenceID{}, uint32(sproto.ServerCmdType_OrderSequenceID))
	namespace.Register(&sproto.OrderSequenceIDResp{}, uint32(sproto.ServerCmdType_OrderSequenceIDResp))

	//for test
	namespace.Register(&sproto.PacketTest{}, uint32(sproto.ServerCmdType_PacketTest))

}
