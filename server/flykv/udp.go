package flykv

import (
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type udpMsg struct {
	from *net.UDPAddr
	m    *snet.Message
}

func (this *kvnode) processUdpMsg(from *net.UDPAddr, m *snet.Message) {
	switch m.Msg.(type) {
	case *sproto.QueryLeader:
		this.muS.RLock()
		store, ok := this.stores[int(m.Msg.(*sproto.QueryLeader).GetStore())]
		this.muS.RUnlock()

		var leader int32

		if ok {
			leader = int32(store.getLeaderNodeID())
		} else {
			GetSugar().Infof("store:%d not in %d", m.Msg.(*sproto.QueryLeader).GetStore(), this.id)
		}

		GetSugar().Infof("store:%d on QueryLeader leader:%d", m.Msg.(*sproto.QueryLeader).GetStore(), leader)

		this.udpConn.SendTo(from, snet.MakeMessage(m.Context, &sproto.QueryLeaderResp{Leader: leader}))
	case *sproto.NotifyAddNode, *sproto.NotifyRemNode:
		var stores []int32
		switch m.Msg.(type) {
		case *sproto.NotifyAddNode:
			stores = m.Msg.(*sproto.NotifyAddNode).Stores
		case *sproto.NotifyRemNode:
			stores = m.Msg.(*sproto.NotifyRemNode).Stores
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

	case *sproto.NotifySlotTransIn, *sproto.NotifySlotTransOut, *sproto.NotifyUpdateMeta:
		var store int
		switch m.Msg.(type) {
		case *sproto.NotifySlotTransIn:
			store = int(m.Msg.(*sproto.NotifySlotTransIn).Store)
		case *sproto.NotifySlotTransOut:
			store = int(m.Msg.(*sproto.NotifySlotTransOut).Store)
		case *sproto.NotifyUpdateMeta:
			store = int(m.Msg.(*sproto.NotifyUpdateMeta).Store)
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

	GetSugar().Infof("flykv:%d start udp service at %s", this.id, service)

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := this.udpConn.ReadFrom(recvbuff)
			if nil != err {
				return
			} else {
				this.processUdpMsg(from, msg.(*snet.Message))
			}
		}
	}()

	return nil
}
