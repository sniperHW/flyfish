package flygate

import (
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"time"
)

type store struct {
	leaderVersion int64
	id            int
	leader        *kvnode
	slots         *bitmap.Bitmap
	setID         int
	waitSend      containerList
}

func (g *gate) storeSend(store *store, request *request, now time.Time) {
	request.store = uint64(store.setID)<<32 + uint64(store.id)
	if nil == store.leader {
		store.waitSend.add(request)
		if store.waitSend.l.Len() == 1 {
			go g.queryLeader(store)
		}
	} else {
		request.leaderVersion = store.leaderVersion
		g.nodeSend(store.leader, request, now)
	}
}

func (g *gate) queryLeader(store *store) {
	g.Lock()
	defer g.Unlock()
	if g.checkStore(store) {
		set := g.sets[store.setID]
		nodes := []string{}
		for _, v := range set.nodes {
			nodes = append(nodes, v.service)
		}
		if len(nodes) > 0 {
			go func() {
				var leader int
				if r, err := snet.UdpCall(nodes, &sproto.QueryLeader{Store: int32(store.id)}, &sproto.QueryLeaderResp{}, time.Second); nil == err {
					leader = int(r.(*sproto.QueryLeaderResp).Leader)
				}
				g.Lock()
				defer g.Unlock()
				if g.checkStore(store) {
					set := g.sets[store.setID]
					if leaderNode := set.nodes[leader]; nil != leaderNode {
						store.leaderVersion++
						store.leader = leaderNode
						now := time.Now()
						for v := store.waitSend.l.Front(); nil != v; v = store.waitSend.l.Front() {
							request := v.Value.(*request)
							request.remove(&store.waitSend)
							request.leaderVersion = store.leaderVersion
							g.nodeSend(store.leader, request, now)
						}
					} else {
						time.AfterFunc(time.Millisecond*100, func() {
							g.queryLeader(store)
						})
					}
				}
			}()
		} else {
			time.AfterFunc(time.Millisecond*100, func() {
				g.queryLeader(store)
			})
		}
	}
}

func (g *gate) onErrNotLeader(store *store, request *request) {
	if nil != store.leader && store.leaderVersion != request.leaderVersion {
		//leader已经变更，向新的leader发送
		request.leaderVersion = store.leaderVersion
		g.nodeSend(store.leader, request, time.Now())
	} else if nil != store.leader && store.leaderVersion == request.leaderVersion {
		store.leader = nil
	}

	if nil == store.leader {
		store.waitSend.add(request)
		if store.waitSend.l.Len() == 1 {
			go g.queryLeader(store)
		}
	}
}
