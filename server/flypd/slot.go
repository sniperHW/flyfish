package flypd

import (
	"fmt"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sort"
	"time"
)

type SlotTransferMgr struct {
	Transactions map[int]*TransSlotTransfer //正在执行的迁移事务
	Plan         map[int]*SlotTransferPlan  //待执行的迁移计划
	FreeSlots    map[int]bool               //无归属的slot
}

func (s *SlotTransferMgr) onSetRemove(pd *pd, ss *Set) {
	for _, st := range ss.Stores {
		for _, b := range st.slots.GetOpenBits() {
			_, onPlan := s.Plan[b]
			_, onTransaction := s.Transactions[b]
			if !onPlan && !onTransaction {
				s.FreeSlots[b] = true
			}
		}
	}

	//清理plan
	for _, v := range s.Plan {
		if v.SetIn == ss.SetID {
			//被删除set是计划迁入的set
			if v.SetOut < 0 {
				//从freeslot迁入，将slot归还给freeslot
				s.FreeSlots[v.Slot] = true
			}
			delete(s.Plan, v.Slot)
		} else if v.SetOut == ss.SetID {
			//被删除set是计划迁出set,将slot放入freeslot
			s.FreeSlots[v.Slot] = true
			delete(s.Plan, v.Slot)
		}

	}

	//清理transaction
	for _, v := range s.Transactions {
		if v.SetIn == ss.SetID {
			//标记迁入set已经失效
			v.SetIn = -1
			if v.SetOut < 0 || v.StoreTransferOutOk {
				//从fresslot迁入或迁出已经完成
				s.FreeSlots[v.Slot] = true
			}

			if v.SetOut < 0 || v.StoreTransferOutOk || !v.ready {
				if nil != v.timer {
					v.timer.Stop()
				}
				delete(s.Transactions, v.Slot)
			}

		} else if v.SetOut == ss.SetID {
			if v.SetIn < 0 {
				//待迁入set已经被移除
				s.FreeSlots[v.Slot] = true
				if nil != v.timer {
					v.timer.Stop()
				}
				delete(s.Transactions, v.Slot)
			} else {
				v.StoreTransferOutOk = true
			}
		}
	}

	GetSugar().Infof("onSetRemove:%d %d", ss.SetID, len(s.Transactions)+len(s.Plan))

	pd.slotBalance()

	GetSugar().Infof("onSetRemove finish")

}

type SlotTransferPlan struct {
	Slot               int
	SetOut             int
	StoreTransferOut   int //迁出store
	StoreTransferOutOk bool
	SetIn              int
	StoreTransferIn    int //迁入store
}

type TransSlotTransfer struct {
	SlotTransferPlan
	context int64
	timer   *time.Timer
	ready   bool
}

func (tst *TransSlotTransfer) notify(pd *pd) {
	if tst != pd.SlotTransferMgr.Transactions[tst.Slot] {
		return
	}

	tst.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答
	if tst.SetOut < 0 || tst.StoreTransferOutOk {
		setIn := pd.Deployment.Sets[tst.SetIn]
		for _, v := range setIn.Nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.Host, v.ServicePort))
			pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransIn{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferIn),
				}))
		}
	} else if tst.SetIn >= 0 && !tst.ready {
		setIn := pd.Deployment.Sets[tst.SetIn]
		for _, v := range setIn.Nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.Host, v.ServicePort))
			pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.IsTransInReady{
					Store: int32(tst.StoreTransferIn),
					Slot:  int32(tst.Slot),
				}))
		}
	} else {
		setOut := pd.Deployment.Sets[tst.SetOut]
		for _, v := range setOut.Nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.Host, v.ServicePort))
			pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransOut{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferOut),
				}))
		}
	}

	tst.timer = time.AfterFunc(time.Millisecond*50, func() {
		pd.mainque.AppendHighestPriotiryItem(tst)
	})
}

var CurrentTransferOutCount int = 6

func (p *pd) slotBalance() {

	if len(p.Deployment.Sets) == 0 {
		return
	}

	if len(p.SlotTransferMgr.Plan)+len(p.SlotTransferMgr.Transactions) == 0 {

		type st struct {
			setId   int
			storeId int
			slots   []int
		}

		stores := []*st{}
		markClearStore := []*st{}
		for _, v := range p.Deployment.Sets {
			for _, vv := range v.Stores {
				if v.MarkClear {
					markClearStore = append(markClearStore, &st{
						setId:   v.SetID,
						storeId: vv.StoreID,
						slots:   vv.slots.GetOpenBits(),
					})
				} else {
					stores = append(stores, &st{
						setId:   v.SetID,
						storeId: vv.StoreID,
						slots:   vv.slots.GetOpenBits(),
					})
				}
			}
		}

		sortStores := func() {
			sort.Slice(stores, func(i int, j int) bool {
				l, r := stores[i], stores[j]
				if len(l.slots) < len(r.slots) {
					return true
				} else if len(l.slots) == len(r.slots) {
					if l.setId < r.setId {
						return true
					} else if l.setId == r.setId {
						return l.storeId < r.storeId
					}
				}
				return false
			})
		}

		sort.Slice(markClearStore, func(i int, j int) bool {
			l, r := markClearStore[i], markClearStore[j]
			if l.setId < r.setId {
				return true
			} else if l.setId == r.setId {
				return l.storeId < r.storeId
			} else {
				return false
			}
		})

		if len(p.SlotTransferMgr.FreeSlots) > 0 || len(markClearStore) > 0 {
			for k, _ := range p.SlotTransferMgr.FreeSlots {
				sortStores()
				less := stores[0]
				less.slots = append(less.slots, k)
				p.SlotTransferMgr.Plan[k] = &SlotTransferPlan{
					Slot:               k,
					SetOut:             -1,
					StoreTransferOut:   -1,
					StoreTransferOutOk: true,
					SetIn:              less.setId,
					StoreTransferIn:    less.storeId,
				}
				delete(p.SlotTransferMgr.FreeSlots, k)
			}

			for _, v := range markClearStore {
				for _, s := range v.slots {
					sortStores()
					less := stores[0]
					less.slots = append(less.slots, s)
					p.SlotTransferMgr.Plan[s] = &SlotTransferPlan{
						Slot:             s,
						SetOut:           v.setId,
						StoreTransferOut: v.storeId,
						SetIn:            less.setId,
						StoreTransferIn:  less.storeId,
					}
				}
			}
		} else if len(stores) > 1 {
			storeAverageSlotCount := slot.SlotCount / len(stores)

			//GetSugar().Infof("storeAverageSlotCount:%d", storeAverageSlotCount)

			for {
				sortStores()
				if len(stores[0].slots) >= storeAverageSlotCount {
					break
				}
				less := stores[0]
				more := stores[len(stores)-1]
				slot := more.slots[0]
				more.slots = more.slots[1:]
				less.slots = append(less.slots, slot)
				p.SlotTransferMgr.Plan[slot] = &SlotTransferPlan{
					Slot:             slot,
					SetOut:           more.setId,
					StoreTransferOut: more.storeId,
					SetIn:            less.setId,
					StoreTransferIn:  less.storeId,
				}
			}
		}
	}

	if p.isLeader() {

		plan := []*SlotTransferPlan{}
		for _, v := range p.SlotTransferMgr.Plan {
			plan = append(plan, v)
		}

		/* 排序规则SetOut小优先
		 * 如果SetOut相等,则Slot小优先
		 */
		sort.Slice(plan, func(i int, j int) bool {
			l, r := plan[i], plan[j]
			if l.SetOut < r.SetOut {
				return true
			} else if l.SetOut == r.SetOut {
				return l.Slot < r.Slot
			} else {
				return false
			}
		})

		//避免大量transferout导致大量slot处于无法服务状态
		transferOutCount := 0

		for _, v := range p.SlotTransferMgr.Transactions {
			if v.SetOut >= 0 {
				transferOutCount++
			}
		}

		for _, v := range plan {
			if transferOutCount < CurrentTransferOutCount {
				delete(p.SlotTransferMgr.Plan, v.Slot)
				t := &TransSlotTransfer{
					SlotTransferPlan: SlotTransferPlan{
						Slot:             v.Slot,
						SetOut:           v.SetOut,
						StoreTransferOut: v.StoreTransferOut,
						SetIn:            v.SetIn,
						StoreTransferIn:  v.StoreTransferIn,
					},
				}
				p.SlotTransferMgr.Transactions[v.Slot] = t
				t.notify(p)
				transferOutCount++
			} else {
				break
			}
		}
	}

	/*if len(p.pState.SlotTransferMgr.Plan)+len(p.pState.SlotTransferMgr.Transactions)+len(p.pState.SlotTransferMgr.FreeSlots) == 0 {
		s := [][]int{}
		for _, v := range p.pState.deployment.sets {
			for _, vv := range v.stores {
				s = append(s, vv.slots.GetOpenBits())
			}
		}
		GetSugar().Infof("slotBalance finish %v", s)
	}*/
}

type ProposalSlotTransOutOk struct {
	proposalBase
	Slot int
}

func (p *ProposalSlotTransOutOk) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSlotTransOutOk, p)
}

func (p *ProposalSlotTransOutOk) apply(pd *pd) {
	if t, ok := pd.SlotTransferMgr.Transactions[p.Slot]; ok {
		if !t.StoreTransferOutOk {
			t.StoreTransferOutOk = true
			set := pd.Deployment.Sets[t.SetOut]
			store := set.Stores[t.StoreTransferOut]
			store.slots.Clear(int(t.Slot))
			store.Slots = store.slots.ToJson()
			pd.Deployment.Version++
			set.Version = pd.Deployment.Version
			if t.SetIn < 0 {
				//迁入set已经被删除,将已经迁出的slot放入freeslot
				delete(pd.SlotTransferMgr.Transactions, p.Slot)
				pd.SlotTransferMgr.FreeSlots[p.Slot] = true

				if t.timer != nil {
					t.timer.Stop()
				}

				pd.slotBalance()
			} else {

				if t.timer != nil {
					t.timer.Stop()
				}

				if pd.isLeader() {
					t.notify(pd)
				}
			}
		}
	}
}

type ProposalSlotTransInOk struct {
	proposalBase
	Slot int
}

func (p *ProposalSlotTransInOk) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSlotTransInOk, p)
}

func (p *ProposalSlotTransInOk) apply(pd *pd) {
	if t, ok := pd.SlotTransferMgr.Transactions[p.Slot]; ok {
		delete(pd.SlotTransferMgr.Transactions, p.Slot)
		if nil != t.timer {
			t.timer.Stop()
		}

		if t.SetIn < 0 {
			pd.SlotTransferMgr.FreeSlots[p.Slot] = true
		} else {
			set := pd.Deployment.Sets[t.SetIn]
			store := set.Stores[t.StoreTransferIn]
			store.slots.Set(int(t.Slot))
			store.Slots = store.slots.ToJson()
			pd.Deployment.Version++
			set.Version = pd.Deployment.Version
		}
		pd.slotBalance()
	}
}

func (p *pd) onSlotTransInReady(_ replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.IsTransInReadyResp)
	if t, ok := p.SlotTransferMgr.Transactions[int(msg.Slot)]; ok && t.context == m.Context {
		if msg.Ready && t.ready == false {
			t.ready = true
			if t.timer == nil || t.timer.Stop() {
				t.notify(p)
			}
		}
	}
}

func (p *pd) onSlotTransOutOk(_ replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.SlotTransOutOk)
	if t, ok := p.SlotTransferMgr.Transactions[int(msg.Slot)]; ok && t.context == m.Context {
		if !t.StoreTransferOutOk {
			p.issueProposal(&ProposalSlotTransOutOk{
				Slot: int(msg.Slot),
			})
		}
	}
}

func (p *pd) onSlotTransInOk(_ replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.SlotTransInOk)
	if t, ok := p.SlotTransferMgr.Transactions[int(msg.Slot)]; ok && t.context == m.Context {
		p.issueProposal(&ProposalSlotTransInOk{
			Slot: int(msg.Slot),
		})
	}
}
