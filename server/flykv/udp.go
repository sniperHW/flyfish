package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
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
	case *sproto.NotifyMissingStores:

		GetSugar().Errorf("NotifyMissingStores")

		msg := m.Msg.(*sproto.NotifyMissingStores)

		dbdef, err := db.MakeDbDefFromJsonString(msg.Meta)

		if nil != err {
			GetSugar().Errorf("NotifyMissingStores CreateDbDefFromJsonString err:%v", err)
			return
		}

		meta, err := sql.CreateDbMeta(dbdef)

		if nil != err {
			GetSugar().Errorf("NotifyMissingStores CreateDbMeta err:%v", err)
			return
		}

		for _, v := range msg.Stores {
			slots, err := bitmap.CreateFromJson(v.Slots)
			if nil != err {
				GetSugar().Errorf("NotifyMissingStores CreateFromJson store:%d err:%v", v.Id, err)
				return
			}

			peers, err := raft.SplitPeers(v.RaftCluster)

			if nil != err {
				GetSugar().Errorf("NotifyMissingStores SplitPeers store:%d err:%v,origin:%v", v.Id, err, v.RaftCluster)
				return
			}

			if err = this.addStore(meta, int(v.Id), peers, slots); nil != err {
				GetSugar().Errorf("NotifyMissingStores addStore store:%d err:%v", v.Id, err)
				return
			}
		}
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
			case *sproto.ClearStoreCache:
				store = int(m.Msg.(*sproto.ClearStoreCache).Store)
			case *sproto.TrasnferLeader:
				store = int(m.Msg.(*sproto.TrasnferLeader).StoreID)
			case *sproto.SuspendStore:
				store = int(m.Msg.(*sproto.SuspendStore).Store)
			case *sproto.ResumeStore:
				store = int(m.Msg.(*sproto.ResumeStore).Store)
			default:
				return
			}

			this.muS.RLock()
			if s, ok := this.stores[int(store)]; ok {
				s.mainQueue.AppendHighestPriotiryItem(&udpMsg{
					from: from,
					m:    m,
				})
			} // else {
			//	GetSugar().Infof("store:%d not found %v node:%d", store, this.stores, this.id)
			//}
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
