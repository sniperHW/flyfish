package flygate

import (
	"container/list"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
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

func (s *store) onCliMsg(msg *forwordMsg) {
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
			s.leader.sendForwordMsg(msg)
		}
	}
}

func (s *store) onErrNotLeader(msg *forwordMsg) {
	if s.set.removed {
		return
	}

	if nil != s.leader && s.leaderVersion != msg.leaderVersion {
		//向新的leader发送
		msg.leaderVersion = s.leaderVersion
		s.leader.sendForwordMsg(msg)
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
		nodes := []string{}
		for _, v := range s.set.nodes {
			if !v.removed {
				nodes = append(nodes, v.service)
			}
		}

		if len(nodes) > 0 {
			go func() {
				var leader int
				if resp := snet.UdpCall(nodes, &sproto.QueryLeader{Store: int32(s.id)}, time.Second, func(respCh chan interface{}, r proto.Message) {
					if resp, ok := r.(*sproto.QueryLeaderResp); ok && 0 != resp.Leader {
						select {
						case respCh <- int(resp.Leader):
						default:
						}
					}
				}); nil != resp {
					leader = resp.(int)
				}

				s.mainQueue.ForceAppend(1, func() {
					s.queryingLeader = false
					if leaderNode, ok := s.set.nodes[leader]; ok {
						s.leaderVersion++
						s.leader = leaderNode
						GetSugar().Infof("store:%d got leader:%d", s.id, leader)
						for v := s.waittingSend.Front(); nil != v; v = s.waittingSend.Front() {
							msg := s.waittingSend.Remove(v).(*forwordMsg)
							msg.leaderVersion = s.leaderVersion
							msg.l = nil
							msg.listElement = nil
							leaderNode.sendForwordMsg(msg)
						}
					} else {
						time.AfterFunc(time.Millisecond*100, func() {
							s.mainQueue.ForceAppend(1, s.queryLeader)
						})
					}
				})

			}()
		} else {
			time.AfterFunc(time.Second, func() {
				s.mainQueue.ForceAppend(1, s.queryLeader)
			})
		}
	}
}
