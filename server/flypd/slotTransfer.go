package flypd

import (
	"fmt"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

type TransSlotTransfer struct {
	Slot               int
	SetOut             int
	StoreTransferOut   int //迁出store
	StoreTransferOutOk bool
	SetIn              int
	StoreTransferIn    int //迁入store
	context            int64
	timer              *time.Timer
	ready              bool
}

func (tst *TransSlotTransfer) notify(pd *pd) {
	tst.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答
	if !tst.StoreTransferOutOk {
		if !tst.ready {
			/*
			 *  如果迁入store尚未启动或没有leader，则不应该启动迁出，否则迁移slot将没有store装载，导致无法服务
			 *  因此，启动迁出之前应该先询问待迁入store是否已经准备好迁入。
			 */
			setIn := pd.pState.deployment.sets[tst.SetIn]
			for _, v := range setIn.nodes {
				addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
				pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
					&sproto.IsTransInReady{
						Store: int32(tst.StoreTransferIn),
						Slot:  int32(tst.Slot),
					}))
			}
		} else {
			setOut := pd.pState.deployment.sets[tst.SetOut]
			for _, v := range setOut.nodes {
				addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
				pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
					&sproto.NotifySlotTransOut{
						Slot:  int32(tst.Slot),
						Store: int32(tst.StoreTransferOut),
					}))
			}
		}
	} else {
		setIn := pd.pState.deployment.sets[tst.SetIn]
		for _, v := range setIn.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransIn{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferIn),
				}))
		}
	}

	tst.timer = time.AfterFunc(time.Second, func() {
		pd.mainque.AppendHighestPriotiryItem(tst)
	})
}

type ProposalBeginSlotTransfer struct {
	proposalBase
	Trans *TransSlotTransfer
}

func (p *ProposalBeginSlotTransfer) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalBeginSlotTransfer, p)
}

func (p *ProposalBeginSlotTransfer) doApply(pd *pd) bool {
	if _, ok := pd.pState.SlotTransfer[p.Trans.Slot]; !ok {
		pd.pState.SlotTransfer[p.Trans.Slot] = p.Trans
		return true
	} else {
		return false
	}
}

func (p *ProposalBeginSlotTransfer) apply(pd *pd) {
	if p.doApply(pd) {
		p.Trans.notify(pd)
	}
}

func (p *ProposalBeginSlotTransfer) replay(pd *pd) {
	p.doApply(pd)
}

type ProposalSlotTransOutOk struct {
	proposalBase
	Slot int
}

func (p *ProposalSlotTransOutOk) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSlotTransOutOk, p)
}

func (p *ProposalSlotTransOutOk) apply(pd *pd) {
	if t, ok := pd.pState.SlotTransfer[p.Slot]; ok {
		if !t.StoreTransferOutOk {
			t.StoreTransferOutOk = true
			s := pd.pState.deployment.sets[t.SetOut]
			st := s.stores[t.StoreTransferOut]
			st.slots.Clear(int(t.Slot))
			pd.pState.deployment.version++
			s.version = pd.pState.deployment.version
			//迁出已经完成，通知迁入
			if t.timer == nil || t.timer.Stop() {
				t.notify(pd)
			}
		}
	}
}

func (p *ProposalSlotTransOutOk) replay(pd *pd) {
	p.apply(pd)
}

type ProposalSlotTransInOk struct {
	proposalBase
	Slot int
}

func (p *ProposalSlotTransInOk) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSlotTransInOk, p)
}

func (p *ProposalSlotTransInOk) doApply(pd *pd) bool {
	if t, ok := pd.pState.SlotTransfer[p.Slot]; ok {
		delete(pd.pState.SlotTransfer, p.Slot)
		s := pd.pState.deployment.sets[t.SetIn]
		st := s.stores[t.StoreTransferIn]
		st.slots.Set(int(t.Slot))
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
		if nil != t.timer {
			t.timer.Stop()
		}
		return true
	} else {
		return false
	}
}

func (p *ProposalSlotTransInOk) apply(pd *pd) {
	if p.doApply(pd) {
		pd.slotBalance()
	}
}

func (p *ProposalSlotTransInOk) replay(pd *pd) {
	p.doApply(pd)
}

func (p *pd) beginSlotTransfer(slot int, setOut int, storeOut int, setIn int, storeIn int) {

	GetSugar().Infof("beginSlotTransfer slot:%d setOut:%d setIn:%d storeOut:%d storeIn:%d", slot, setOut, setIn, storeOut, storeIn)

	p.issueProposal(&ProposalBeginSlotTransfer{
		Trans: &TransSlotTransfer{
			Slot:             slot,
			SetOut:           setOut,
			StoreTransferOut: storeOut,
			SetIn:            setIn,
			StoreTransferIn:  storeIn,
		},
	})
}

func (p *pd) onSlotTransInReady(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.IsTransInReadyResp)
	if t, ok := p.pState.SlotTransfer[int(msg.Slot)]; ok && t.context == m.Context {
		if msg.Ready && t.ready == false {
			t.ready = true
			if t.timer == nil || t.timer.Stop() {
				t.notify(p)
			}
		}
	}
}

func (p *pd) onSlotTransOutOk(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SlotTransOutOk)
	if t, ok := p.pState.SlotTransfer[int(msg.Slot)]; ok && t.context == m.Context {
		if !t.StoreTransferOutOk {
			p.issueProposal(&ProposalSlotTransOutOk{
				Slot: int(msg.Slot),
			})

		}
	}
}

func (p *pd) onSlotTransInOk(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SlotTransInOk)
	if t, ok := p.pState.SlotTransfer[int(msg.Slot)]; ok && t.context == m.Context {
		p.issueProposal(&ProposalSlotTransInOk{
			Slot: int(msg.Slot),
		})
	}
}
