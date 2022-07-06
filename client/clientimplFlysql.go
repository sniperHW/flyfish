package client

import (
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

func QueryFlysql(pd []*net.UDPAddr, timeout time.Duration) (ret []string) {
	if resp, _ := snet.UdpCall(pd, &sproto.GetFlySqlList{}, &sproto.GetFlySqlListResp{}, timeout); nil != resp {
		ret = resp.(*sproto.GetFlySqlListResp).List
	}
	return ret
}

type clientImplFlySql struct {
	impl
}

func (this *clientImplFlySql) queryFlysql(pdAddr []*net.UDPAddr) {
	go func() {
		closed, delay := this.onQueryServiceResp(QueryFlysql(pdAddr, time.Second))
		if closed {
			return
		} else {
			time.AfterFunc(delay, func() {
				this.queryFlysql(pdAddr)
			})
		}
	}()
}

func (this *clientImplFlySql) start(pdAddr []*net.UDPAddr) {
	this.queryFlysql(pdAddr)
}
