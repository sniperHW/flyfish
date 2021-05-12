package net

import (
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/core/buffer"
	"github.com/sniperHW/flyfish/net/pb"
	pdproto "github.com/sniperHW/flyfish/server/pd/proto"
	"net"
)

func Unpack(b []byte) (msg proto.Message, err error) {
	r := buffer.NewReader(b)
	cmd := r.GetUint16()
	msg, err = pb.GetNamespace("pd").Unmarshal(uint32(cmd), b[2:])
	return
}

func Pack(msg proto.Message) ([]byte, error) {
	if data, cmd, err := pb.GetNamespace("pd").Marshal(msg); nil != err {
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

type Udp struct {
	address string
	addr    *net.UDPAddr
	conn    *net.UDPConn
}

func NewUdp(service string) (*Udp, error) {
	addr, err := net.ResolveUDPAddr("udp", service)
	if nil != err {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Udp{
		address: service,
		addr:    addr,
		conn:    conn,
	}, nil
}

func (u *Udp) SendTo(addr *net.UDPAddr, msg proto.Message) error {
	if b, err := Pack(msg); nil != err {
		return err
	} else {
		_, err := u.conn.WriteToUDP(b, addr)
		return err
	}
}

func (u *Udp) ReadFrom(recvBuff []byte) (*net.UDPAddr, proto.Message, error) {
	n, addr, err := u.conn.ReadFromUDP(recvBuff)
	if nil != err {
		return addr, nil, err
	}

	msg, err := Unpack(recvBuff[:n])
	return addr, msg, err
}

func (u *Udp) Close() {
	u.conn.Close()
}

func init() {
	namespace := pb.GetNamespace("pd")

	namespace.Register(&pdproto.KvnodeBoot{}, uint32(pdproto.CmdType_KvnodeBoot))
	namespace.Register(&pdproto.KvnodeBootResp{}, uint32(pdproto.CmdType_KvnodeBootResp))

	namespace.Register(&pdproto.AddKvnode{}, uint32(pdproto.CmdType_AddKvnode))
	namespace.Register(&pdproto.AddKvnodeResp{}, uint32(pdproto.CmdType_AddKvnodeResp))

	namespace.Register(&pdproto.RemKvnode{}, uint32(pdproto.CmdType_RemKvnode))
	namespace.Register(&pdproto.RemKvnodeResp{}, uint32(pdproto.CmdType_RemKvnodeResp))

	namespace.Register(&pdproto.AddStore{}, uint32(pdproto.CmdType_AddStore))
	namespace.Register(&pdproto.AddStoreResp{}, uint32(pdproto.CmdType_AddStoreResp))

	namespace.Register(&pdproto.RemStore{}, uint32(pdproto.CmdType_RemStore))
	namespace.Register(&pdproto.RemStoreResp{}, uint32(pdproto.CmdType_RemStoreResp))

	namespace.Register(&pdproto.KvnodeAddStore{}, uint32(pdproto.CmdType_KvnodeAddStore))
	namespace.Register(&pdproto.KvnodeAddStoreResp{}, uint32(pdproto.CmdType_KvnodeAddStoreResp))

	namespace.Register(&pdproto.KvnodeRemStore{}, uint32(pdproto.CmdType_KvnodeRemStore))
	namespace.Register(&pdproto.KvnodeRemStoreResp{}, uint32(pdproto.CmdType_KvnodeRemStoreResp))

	namespace.Register(&pdproto.NotifyKvnodeStoreTrans{}, uint32(pdproto.CmdType_NotifyKvnodeStoreTrans))
	namespace.Register(&pdproto.NotifyKvnodeStoreTransResp{}, uint32(pdproto.CmdType_NotifyKvnodeStoreTransResp))

	namespace.Register(&pdproto.SlotTransferPrepare{}, uint32(pdproto.CmdType_SlotTransferPrepare))
	namespace.Register(&pdproto.SlotTransferPrepareAck{}, uint32(pdproto.CmdType_SlotTransferPrepareAck))

	namespace.Register(&pdproto.SlotTransferCancel{}, uint32(pdproto.CmdType_SlotTransferCancel))
	namespace.Register(&pdproto.SlotTransferCommit{}, uint32(pdproto.CmdType_SlotTransferCommit))

}
