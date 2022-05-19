package flygate

import (
	"container/list"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"time"
)

type store struct {
	id             int
	queryingLeader bool
	leaderVersion  int64
	leader         *kvnode
	waittingSend   *list.List //查询leader时的暂存队列
	slots          *bitmap.Bitmap
	set            *set
	removed        bool
	config         *Config
	gate           *gate
}

func (s *store) onCliMsg(msg *forwordMsg) {
	msg.store = s
	if nil == s.leader {
		msg.add(nil, s.waittingSend)
		if s.waittingSend.Len() == 1 {
			s.queryLeader()
		}
	} else {
		msg.leaderVersion = s.leaderVersion
		s.leader.sendForwordMsg(msg)
	}
}

func (s *store) paybackWaittingSendToGate() {
	for v := s.waittingSend.Front(); nil != v; v = s.waittingSend.Front() {
		msg := v.Value.(*forwordMsg)
		msg.removeList()
		msg.add(nil, s.gate.pendingMsg)
	}
}

func (s *store) onErrNotLeader(msg *forwordMsg) {
	//GetSugar().Infof("onErrNotLeader")
	if s.removed {
		msg.add(nil, s.gate.pendingMsg)
	} else {
		if nil != s.leader && s.leaderVersion != msg.leaderVersion {
			//leader已经变更，向新的leader发送
			msg.leaderVersion = s.leaderVersion
			s.leader.sendForwordMsg(msg)
		} else if nil != s.leader && s.leaderVersion == msg.leaderVersion {
			s.leader = nil
		}

		if nil == s.leader {
			//还没有leader,重新投入到待发送队列
			msg.add(nil, s.waittingSend)
			if s.waittingSend.Len() == 1 {
				s.queryLeader()
			}
		}
		return
	}
}

func (s *store) queryLeader() {
	if s.removed {
		s.paybackWaittingSendToGate()
	} else {

		nodes := []string{}
		for _, v := range s.set.nodes {
			if !v.removed {
				nodes = append(nodes, v.service)
			}
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
					if s.removed {
						s.paybackWaittingSendToGate()
					} else if leaderNode, ok := s.set.nodes[leader]; ok {
						s.leaderVersion++
						s.leader = leaderNode
						//GetSugar().Infof("set:%d store:%d got leader nodeID:%d", s.set.setID, s.id, leader)
						for v := s.waittingSend.Front(); nil != v; v = s.waittingSend.Front() {
							msg := v.Value.(*forwordMsg)
							msg.leaderVersion = s.leaderVersion
							msg.removeList()
							leaderNode.sendForwordMsg(msg)
						}
					} else {
						s.gate.afterFunc(time.Millisecond*100, s.queryLeader)
					}
				})

			}()
		} else {
			s.gate.afterFunc(time.Second, s.queryLeader)
		}
	}
}
