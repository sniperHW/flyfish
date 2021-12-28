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
}

func (tst *TransSlotTransfer) notify(pd *pd) {
	tst.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答
	if !tst.StoreTransferOutOk {
		setOut := pd.pState.deployment.sets[tst.SetOut]
		for _, v := range setOut.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransOut{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferOut),
				}))
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

func (p *ProposalBeginSlotTransfer) apply(pd *pd) {
	if _, ok := pd.pState.SlotTransfer[p.Trans.Slot]; !ok {
		pd.pState.SlotTransfer[p.Trans.Slot] = p.Trans

		storeIn := pd.pState.deployment.sets[p.Trans.SetIn].stores[p.Trans.StoreTransferIn]
		storeIn.slotInCount++
		storeIn.set.slotInCount++

		storeOut := pd.pState.deployment.sets[p.Trans.SetOut].stores[p.Trans.StoreTransferOut]
		storeOut.slotOutCount++
		storeOut.set.slotOutCount++

		p.Trans.notify(pd)
	}
}

func (p *ProposalBeginSlotTransfer) replay(pd *pd) {
	if _, ok := pd.pState.SlotTransfer[p.Trans.Slot]; !ok {
		pd.pState.SlotTransfer[p.Trans.Slot] = p.Trans
	}
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
			s.slotOutCount--
			st := s.stores[t.StoreTransferOut]
			st.slotOutCount--
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
	if t, ok := pd.pState.SlotTransfer[p.Slot]; ok {
		t.StoreTransferOutOk = true
		s := pd.pState.deployment.sets[t.SetOut]
		st := s.stores[t.StoreTransferOut]
		st.slots.Clear(int(t.Slot))
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
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
	if t, ok := pd.pState.SlotTransfer[p.Slot]; ok {
		delete(pd.pState.SlotTransfer, p.Slot)
		s := pd.pState.deployment.sets[t.SetIn]
		s.slotInCount--
		st := s.stores[t.StoreTransferIn]
		st.slotInCount--
		st.slots.Set(int(t.Slot))
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
		if nil != t.timer {
			t.timer.Stop()
		}
		pd.slotBalance()
	}
}

func (p *ProposalSlotTransInOk) replay(pd *pd) {
	if t, ok := pd.pState.SlotTransfer[p.Slot]; ok {
		delete(pd.pState.SlotTransfer, p.Slot)
		s := pd.pState.deployment.sets[t.SetIn]
		st := s.stores[t.StoreTransferIn]
		st.slots.Set(int(t.Slot))
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
	}
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
