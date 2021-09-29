package flykv

import (
	"github.com/gogo/protobuf/proto"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type consoleMsg struct {
	from *net.UDPAddr
	m    proto.Message
}

func (this *kvnode) processConsoleMsg(from *net.UDPAddr, m proto.Message) {
	switch m.(type) {
	case *sproto.QueryLeader:
		this.muS.RLock()
		store, ok := this.stores[int(m.(*sproto.QueryLeader).GetStore())]
		this.muS.RUnlock()
		if !ok {
			this.consoleConn.SendTo(from, &sproto.QueryLeaderResp{Yes: false})
		} else {
			this.consoleConn.SendTo(from, &sproto.QueryLeaderResp{Yes: store.isLeader()})
		}
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
				s.mainQueue.AppendHighestPriotiryItem(&consoleMsg{
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
			s.mainQueue.AppendHighestPriotiryItem(&consoleMsg{
				from: from,
				m:    m,
			})
		}
		this.muS.RUnlock()
	}
}

func (this *kvnode) initConsole(service string) error {
	var err error
	this.consoleConn, err = fnet.NewUdp(service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := this.consoleConn.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				this.processConsoleMsg(from, msg)
			}
		}
	}()

	return nil
}
