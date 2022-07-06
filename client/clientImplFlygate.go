package client

import (
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

func QueryGate(pd []*net.UDPAddr, timeout time.Duration) (ret []string) {
	if resp, _ := snet.UdpCall(pd, &sproto.GetFlyGateList{}, &sproto.GetFlyGateListResp{}, timeout); nil != resp {
		ret = resp.(*sproto.GetFlyGateListResp).List
	}
	return ret
}

type clientImplFlyGate struct {
	impl
}

func (this *clientImplFlyGate) queryGate(pdAddr []*net.UDPAddr) {
	go func() {
		closed, delay := this.onQueryServiceResp(QueryGate(pdAddr, time.Second))
		if closed {
			return
		} else {
			time.AfterFunc(delay, func() {
				this.queryGate(pdAddr)
			})
		}
	}()
}

func (this *clientImplFlyGate) start(pdAddr []*net.UDPAddr) {
	this.queryGate(pdAddr)
}
