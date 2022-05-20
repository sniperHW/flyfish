package flygate

import (
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"time"
)

type store struct {
	cache
	leaderVersion int64
	id            int
	leader        *kvnode
	slots         *bitmap.Bitmap
	setID         int
	gate          *gate
}

func (s *store) onCliMsg(msg *forwordMsg) {
	msg.store = uint64(s.setID)<<32 + uint64(s.id)
	if nil == s.leader {
		if s.addMsg(msg) == 1 {
			s.queryLeader()
		}
	} else {
		msg.leaderVersion = s.leaderVersion
		s.leader.sendForwordMsg(msg)
	}
}

func (s *store) paybackWaittingSendToGate() {
	for v := s.l.Front(); nil != v; v = s.l.Front() {
		msg := v.Value.(*forwordMsg)
		s.removeMsg(msg)
		s.gate.paybackMsg(msg)
	}
}

func (s *store) onErrNotLeader(msg *forwordMsg) {
	if nil != s.leader && s.leaderVersion != msg.leaderVersion {
		//leader已经变更，向新的leader发送
		msg.leaderVersion = s.leaderVersion
		s.leader.sendForwordMsg(msg)
	} else if nil != s.leader && s.leaderVersion == msg.leaderVersion {
		s.leader = nil
	}

	if nil == s.leader {
		//还没有leader,重新投入到待发送队列
		if s.addMsg(msg) == 1 {
			s.queryLeader()
		}
	}
}

func (s *store) queryLeader() {
	if !s.gate.checkStore(s) {
		s.paybackWaittingSendToGate()
	} else {

		set := s.gate.sets[s.setID]
		nodes := []string{}
		for _, v := range set.nodes {
			nodes = append(nodes, v.service)
		}

		if len(nodes) > 0 {
			go func() {
				var leader int
				context := snet.MakeUniqueContext()
				if resp := snet.UdpCall(nodes, snet.MakeMessage(context, &sproto.QueryLeader{Store: int32(s.id)}), time.Second, func(respCh chan interface{}, r interface{}) {
					if m, ok := r.(*snet.Message); ok {
						if resp, ok := m.Msg.(*sproto.QueryLeaderResp); ok && context == m.Context && 0 != resp.Leader {
							select {
							case respCh <- int(resp.Leader):
							default:
							}
						}
					}
				}); nil != resp {
					leader = resp.(int)
				}
				s.gate.callInQueue(1, func() {
					if !s.gate.checkStore(s) {
						s.paybackWaittingSendToGate()
					} else {
						set := s.gate.sets[s.setID]
						if leaderNode := set.nodes[leader]; nil != leaderNode {
							s.leaderVersion++
							s.leader = leaderNode
							for v := s.l.Front(); nil != v; v = s.l.Front() {
								msg := v.Value.(*forwordMsg)
								msg.leaderVersion = s.leaderVersion
								s.removeMsg(msg)
								leaderNode.sendForwordMsg(msg)
							}
						} else {
							s.gate.afterFunc(time.Millisecond*100, s.queryLeader)
						}
					}
				})
			}()
		} else {
			s.gate.afterFunc(time.Second, s.queryLeader)
		}
	}
}
