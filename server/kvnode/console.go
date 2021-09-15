package kvnode

import (
	"github.com/gogo/protobuf/proto"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

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
