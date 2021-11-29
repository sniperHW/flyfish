package flygate

import (
	"container/list"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sync/atomic"
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
	config         *Config
	mainQueue      *queue.PriorityQueue
}

func (s *store) onCliMsg(msg *relayMsg) {
	if atomic.AddInt64(msg.totalPendingMsg, 1) > int64(s.config.MaxPendingMsg) {
		msg.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
	} else {
		if nil == s.leader {
			if s.waittingSend.Len() >= s.config.MaxStorePendingMsg {
				msg.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
			} else {
				msg.l = s.waittingSend
				msg.listElement = s.waittingSend.PushBack(msg)
				s.queryLeader()
			}
		} else {
			msg.leaderVersion = s.leaderVersion
			s.leader.sendRelayMsg(msg)
		}
	}
}

func (s *store) onErrNotLeader(msg *relayMsg) {
	if s.set.removed {
		return
	}

	if nil != s.leader && s.leaderVersion != msg.leaderVersion {
		//向新的leader发送
		msg.leaderVersion = s.leaderVersion
		s.leader.sendRelayMsg(msg)
	} else if nil != s.leader && s.leaderVersion == msg.leaderVersion {
		s.leader = nil
		s.queryLeader()
	}

	if nil == s.leader {
		//还没有leader,重新投入到待发送队列
		msg.l = s.waittingSend
		msg.listElement = s.waittingSend.PushBack(msg)
	}
}

func (s *store) queryLeader() {
	if s.set.removed {
		//当前set已经被移除
		return
	} else if s.queryingLeader {
		return
	} else {
		s.queryingLeader = true
		nodes := []*kvnode{}
		for _, v := range s.set.nodes {
			if !v.removed {
				nodes = append(nodes, v)
			}
		}

		go func() {
			okCh := make(chan int)
			uu := make([]*flynet.Udp, len(s.set.nodes))
			for k, v := range nodes {
				go func(i int, n *kvnode) {
					u, err := flynet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
					if nil == err {
						u.SendTo(n.udpAddr, &sproto.QueryLeader{Store: int32(s.id)})
						uu[i] = u
						_, r, err := u.ReadFrom(make([]byte, 256))
						GetSugar().Infof("%v resp %v %v", n.udpAddr, r, err)
						if nil == err {
							if resp, ok := r.(*sproto.QueryLeaderResp); ok && 0 != resp.Leader {
								select {
								case okCh <- int(resp.Leader):
								default:
								}
							}
						}
					} else {
						GetSugar().Infof("%v", err)
					}
				}(k, v)
			}

			ticker := time.NewTicker(3 * time.Second)

			var leader int

			select {

			case v := <-okCh:
				leader = v
			case <-ticker.C:

			}
			ticker.Stop()

			for _, v := range uu {
				if nil != v {
					v.Close()
				}
			}

			s.mainQueue.ForceAppend(1, func() {
				s.queryingLeader = false
				if leaderNode, ok := s.set.nodes[leader]; ok {
					s.leaderVersion++
					s.leader = leaderNode
					GetSugar().Infof("store:%d got leader%d", s.id, leader)
					for v := s.waittingSend.Front(); nil != v; v = s.waittingSend.Front() {
						req := s.waittingSend.Remove(v).(*relayMsg)
						req.leaderVersion = s.leaderVersion
						req.l = nil
						req.listElement = nil
						leaderNode.sendRelayMsg(req)
					}
				} else {
					time.AfterFunc(time.Millisecond*100, func() {
						s.mainQueue.ForceAppend(1, s.queryLeader)
					})
				}
			})
		}()
	}
}
