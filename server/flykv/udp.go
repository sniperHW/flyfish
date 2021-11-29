package flykv

import (
	"github.com/gogo/protobuf/proto"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type udpMsg struct {
	from *net.UDPAddr
	m    proto.Message
}

func (this *kvnode) processUdpMsg(from *net.UDPAddr, m proto.Message) {
	switch m.(type) {
	case *sproto.QueryLeader:
		this.muS.RLock()
		store, ok := this.stores[int(m.(*sproto.QueryLeader).GetStore())]
		this.muS.RUnlock()

		var leader int32

		if ok {
			leader = int32(store.getLeaderNodeID())
		} else {
			GetSugar().Infof("store:%d not in %d", m.(*sproto.QueryLeader).GetStore(), this.id)
		}

		GetSugar().Infof("store:%d on QueryLeader leader:%d", m.(*sproto.QueryLeader).GetStore(), leader)

		this.udpConn.SendTo(from, &sproto.QueryLeaderResp{Leader: leader})
	case *sproto.NotifyAddNode, *sproto.NotifyRemNode:
		var stores []int32
		switch m.(type) {
		case *sproto.NotifyAddNode:
			stores = m.(*sproto.NotifyAddNode).Stores
		case *sproto.NotifyRemNode:
			stores = m.(*sproto.NotifyRemNode).Stores
		}

		this.muS.RLock()
		for _, v := range stores {
			if s, ok := this.stores[int(v)]; ok {
				s.mainQueue.AppendHighestPriotiryItem(&udpMsg{
					from: from,
					m:    m,
				})
			}
		}
		this.muS.RUnlock()

	case *sproto.NotifySlotTransIn, *sproto.NotifySlotTransOut:
		var store int
		switch m.(type) {
		case *sproto.NotifySlotTransIn:
			store = int(m.(*sproto.NotifySlotTransIn).Store)
		case *sproto.NotifySlotTransOut:
			store = int(m.(*sproto.NotifySlotTransOut).Store)
		}

		this.muS.RLock()
		if s, ok := this.stores[int(store)]; ok {
			s.mainQueue.AppendHighestPriotiryItem(&udpMsg{
				from: from,
				m:    m,
			})
		}
		this.muS.RUnlock()
	}
}

func (this *kvnode) initUdp(service string) error {
	var err error
	this.udpConn, err = fnet.NewUdp(service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := this.udpConn.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				this.processUdpMsg(from, msg)
			}
		}
	}()

	return nil
}
