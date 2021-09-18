package flygate

import (
	"github.com/gogo/protobuf/proto"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

func (g *gate) processConsoleMsg(from *net.UDPAddr, m proto.Message) {
	switch m.(type) {
	case *sproto.NotifyReloadKvconf:

	}
}

func (g *gate) initConsole(service string) error {
	var err error
	g.consoleConn, err = fnet.NewUdp(service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := g.consoleConn.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				g.processConsoleMsg(from, msg)
			}
		}
	}()

	return nil
}
