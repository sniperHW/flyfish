package gate

import (
	"container/list"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sync"
	"sync/atomic"
	"time"
)

type store struct {
	sync.Mutex
	id             int
	queryingLeader bool
	version        int64
	leader         *node
	nodes          []*node
	waittingSend   *list.List //查询leader时的暂存队列
	gate           *gate
}

func (s *store) onCliMsg(cli *flynet.Socket, msg *relayMsg) {
	GetSugar().Infof("got msg cmd:%d,seq:%d", msg.cmd, msg.seqno)
	var leader *node
	s.Lock()
	if nil == s.leader {
		if s.waittingSend.Len() >= s.gate.config.MaxStorePendingMsg {
			s.Unlock()
			replyCliError(msg.cli, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_gate_busy, ""))
		} else {
			s.waittingSend.PushBack(msg)
			s.queryLeader()
			s.Unlock()
		}
	} else {
		msg.version = atomic.LoadInt64(&s.version)
		leader = s.leader
		s.Unlock()
	}

	if nil != leader {
		leader.sendRelayReq(msg)
	}
}

func (s *store) onLoseLeader(version int64) {
	s.Lock()
	if atomic.LoadInt64(&s.version) == version && nil != s.leader {
		s.leader = nil
	}
	s.Unlock()
}

func (s *store) queryLeader() {
	if s.queryingLeader {
		return
	} else {

		GetSugar().Infof("queryLeader")

		s.queryingLeader = true
		version := atomic.AddInt64(&s.version, 1)
		go func() {
			okCh := make(chan *node)
			uu := make([]*flynet.Udp, len(s.nodes))
			for k, v := range s.nodes {
				go func(n *node) {
					u, err := flynet.NewUdp(fmt.Sprintf("%s:0", s.gate.config.ServiceHost), snet.Pack, snet.Unpack)
					if nil == err {
						u.SendTo(n.consoleAddr, &sproto.QueryLeader{Store: int32(s.id)})
						uu[k] = u
						recvbuff := make([]byte, 256)
						_, r, err := u.ReadFrom(recvbuff)
						if nil == err {
							if resp, ok := r.(*sproto.QueryLeaderResp); ok && resp.Yes {
								okCh <- n
							}
						}
					} else {
						GetSugar().Infof("%v", err)
					}
				}(v)
			}

			ticker := time.NewTicker(3 * time.Second)

			var n *node

			select {

			case v := <-okCh:
				n = v
			case <-ticker.C:

			}
			ticker.Stop()

			for _, v := range uu {
				if nil != v {
					v.Close()
				}
			}

			s.Lock()
			waittingSend := s.waittingSend
			s.waittingSend = list.New()
			s.queryingLeader = false
			if nil != n {
				s.leader = n
				GetSugar().Infof("store:%d got leader:%d", s.id, n.id)
			}
			s.Unlock()

			if nil == n {
				for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
					req := waittingSend.Remove(v).(*relayMsg)
					replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_retry, ""))
				}
			} else {
				for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
					req := waittingSend.Remove(v).(*relayMsg)
					req.version = version
					n.sendRelayReq(req)
				}
			}
		}()
	}
}
