package flypd

import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
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
	pd                 *pd
}

func (tst *TransSlotTransfer) notify() {
	tst.context = snet.MakeUniqueContext() //更新context,后续只接受相应context的应答
	if !tst.StoreTransferOutOk {
		setOut := tst.pd.deployment.sets[tst.SetOut]
		for _, v := range setOut.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			tst.pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransOut{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferOut),
				}))
		}
	} else {
		setIn := tst.pd.deployment.sets[tst.SetIn]
		for _, v := range setIn.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.servicePort))
			tst.pd.udp.SendTo(addr, snet.MakeMessage(tst.context,
				&sproto.NotifySlotTransIn{
					Slot:  int32(tst.Slot),
					Store: int32(tst.StoreTransferIn),
				}))
		}
	}

	tst.timer = time.AfterFunc(time.Second, func() {
		tst.pd.mainque.AppendHighestPriotiryItem(tst)
	})
}

type ProposalBeginSlotTransfer struct {
	*proposalBase
	trans *TransSlotTransfer
}

func (p *ProposalBeginSlotTransfer) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalBeginSlotTransfer))
	bb, err := json.Marshal(p.trans)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalBeginSlotTransfer) apply() {
	if _, ok := p.pd.slotTransfer[p.trans.Slot]; !ok {
		p.pd.slotTransfer[p.trans.Slot] = p.trans

		storeIn := p.pd.deployment.sets[p.trans.SetIn].stores[p.trans.StoreTransferIn]
		storeIn.SlotInCount++
		storeIn.set.SlotInCount++

		storeOut := p.pd.deployment.sets[p.trans.SetOut].stores[p.trans.StoreTransferOut]
		storeOut.SlotOutCount++
		storeOut.set.SlotOutCount++

		p.trans.notify()
	}

	if nil != p.reply {
		p.reply()
	}
}

func (p *pd) replayBeginSlotTransfer(reader *buffer.BufferReader) error {
	var trans TransSlotTransfer
	if err := json.Unmarshal(reader.GetAll(), &trans); nil != err {
		return err
	}
	trans.pd = p
	if _, ok := p.slotTransfer[trans.Slot]; !ok {
		p.slotTransfer[trans.Slot] = &trans
	}
	return nil
}

type ProposalNotifySlotTransOutResp struct {
	*proposalBase
	slot int
}

func (p *ProposalNotifySlotTransOutResp) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalNotifySlotTransOutResp))
	return buffer.AppendInt32(b, int32(p.slot))
}

func (p *ProposalNotifySlotTransOutResp) apply() {
	if t, ok := p.pd.slotTransfer[p.slot]; ok {
		if !t.StoreTransferOutOk {
			t.StoreTransferOutOk = true
			s := p.pd.deployment.sets[t.SetOut]
			s.SlotOutCount--
			st := s.stores[t.StoreTransferOut]
			st.SlotOutCount--
			st.slots.Clear(int(t.Slot))
			p.pd.deployment.version++
			s.version = p.pd.deployment.version
			//迁出已经完成，通知迁入
			if t.timer == nil || t.timer.Stop() {
				t.notify()
			}
		}
	}
}

func (p *pd) replayNotifySlotTransOutResp(reader *buffer.BufferReader) error {
	slot := int(reader.GetInt32())
	if t, ok := p.slotTransfer[slot]; ok {
		t.StoreTransferOutOk = true
		s := p.deployment.sets[t.SetOut]
		st := s.stores[t.StoreTransferOut]
		st.slots.Clear(int(t.Slot))
		p.deployment.version++
		s.version = p.deployment.version
	}
	return nil
}

type ProposalNotifySlotTransInResp struct {
	*proposalBase
	slot int
}

func (p *ProposalNotifySlotTransInResp) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalNotifySlotTransInResp))
	return buffer.AppendInt32(b, int32(p.slot))
}

func (p *ProposalNotifySlotTransInResp) apply() {
	if t, ok := p.pd.slotTransfer[p.slot]; ok {
		delete(p.pd.slotTransfer, p.slot)
		s := p.pd.deployment.sets[t.SetIn]
		s.SlotInCount--
		st := s.stores[t.StoreTransferIn]
		st.SlotInCount--
		st.slots.Set(int(t.Slot))
		p.pd.deployment.version++
		s.version = p.pd.deployment.version
		if nil != t.timer {
			t.timer.Stop()
		}
		p.pd.slotBalance()
	}
}

func (p *pd) replayNotifySlotTransInResp(reader *buffer.BufferReader) error {
	slot := int(reader.GetInt32())
	if t, ok := p.slotTransfer[slot]; ok {
		delete(p.slotTransfer, slot)
		s := p.deployment.sets[t.SetIn]
		st := s.stores[t.StoreTransferIn]
		st.slots.Set(int(t.Slot))
		p.deployment.version++
		s.version = p.deployment.version
	}
	return nil
}

func (p *pd) beginSlotTransfer(slot int, setOut int, storeOut int, setIn int, storeIn int) error {

	GetSugar().Infof("beginSlotTransfer slot:%d setOut:%d setIn:%d storeOut:%d storeIn:%d", slot, setOut, setIn, storeOut, storeIn)

	return p.issueProposal(&ProposalBeginSlotTransfer{
		trans: &TransSlotTransfer{
			Slot:             slot,
			SetOut:           setOut,
			StoreTransferOut: storeOut,
			SetIn:            setIn,
			StoreTransferIn:  storeIn,
			pd:               p,
		},
		proposalBase: &proposalBase{
			pd: p,
		},
	})
}

func (p *pd) onNotifySlotTransOutResp(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.NotifySlotTransOutResp)
	if t, ok := p.slotTransfer[int(msg.Slot)]; ok && t.context == m.Context {
		if !t.StoreTransferOutOk {
			p.issueProposal(&ProposalNotifySlotTransOutResp{
				slot: int(msg.Slot),
				proposalBase: &proposalBase{
					pd: p,
				},
			})

		}
	}
}

func (p *pd) onNotifySlotTransInResp(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.NotifySlotTransInResp)
	if t, ok := p.slotTransfer[int(msg.Slot)]; ok && t.context == m.Context {
		p.issueProposal(&ProposalNotifySlotTransInResp{
			slot: int(msg.Slot),
			proposalBase: &proposalBase{
				pd: p,
			},
		})
	}
}
