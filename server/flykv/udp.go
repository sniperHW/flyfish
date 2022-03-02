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

func (this *kvnode) isVaildPdAddress(addr *net.UDPAddr) bool {
	if this.config.Mode == "solo" {
		return true
	} else {
		for _, v := range this.pdAddr {
			if addr.IP.Equal(v.IP) {
				return true
			}
		}
		return false
	}
}

func (this *kvnode) processUdpMsg(from *net.UDPAddr, m *snet.Message) {
	GetSugar().Debugf("processUdpMsg %v", m.Msg)
	switch m.Msg.(type) {
	case *sproto.QueryLeader:
		this.muS.RLock()
		store, ok := this.stores[int(m.Msg.(*sproto.QueryLeader).Store)]
		this.muS.RUnlock()

		var leader int32

		if ok && store.isReady() {
			leader = int32(this.id)
		} else {
			GetSugar().Debugf("store:%d not in %d", m.Msg.(*sproto.QueryLeader).Store, this.id)
		}
		GetSugar().Debugf("store:%d on QueryLeader leader:%d", m.Msg.(*sproto.QueryLeader).Store, leader)
		this.udpConn.SendTo(from, snet.MakeMessage(m.Context, &sproto.QueryLeaderResp{Leader: leader}))
	default:
		if this.isVaildPdAddress(from) {
			var store int
			switch m.Msg.(type) {
			case *sproto.NotifySlotTransIn:
				store = int(m.Msg.(*sproto.NotifySlotTransIn).Store)
			case *sproto.NotifySlotTransOut:
				store = int(m.Msg.(*sproto.NotifySlotTransOut).Store)
			case *sproto.NotifyUpdateMeta:
				store = int(m.Msg.(*sproto.NotifyUpdateMeta).Store)
			case *sproto.NotifyNodeStoreOp:
				store = int(m.Msg.(*sproto.NotifyNodeStoreOp).Store)
			case *sproto.IsTransInReady:
				store = int(m.Msg.(*sproto.IsTransInReady).Store)
			case *sproto.DrainStore:
				store = int(m.Msg.(*sproto.DrainStore).Store)
			case *sproto.TrasnferLeader:
				store = int(m.Msg.(*sproto.TrasnferLeader).StoreID)
			default:
				return
			}

			this.muS.RLock()
			if s, ok := this.stores[int(store)]; ok {
				s.mainQueue.AppendHighestPriotiryItem(&udpMsg{
					from: from,
					m:    m,
				})
			}
			this.muS.RUnlock()
		} else {
			GetSugar().Errorf("invaild udp message from:%s", from.String())
		}
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
