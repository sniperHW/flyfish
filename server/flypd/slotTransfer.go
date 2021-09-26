package flypd

import (
	"encoding/json"
	"time"
	//"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type TransSlotTransfer struct {
	Slot               int
	SetOut             int
	StoreTransferOut   int //迁出store
	StoreTransferOutOk bool
	SetIn              int
	StoreTransferIn    int //迁入store
	timer              *time.Timer
	pd                 *pd
}

func (tst *TransSlotTransfer) notify() {
	if !tst.StoreTransferOutOk {
		setOut := tst.pd.deployment.sets[tst.SetOut]
		for _, v := range setOut.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.interPort))
			tst.pd.udp.SendTo(addr, &sproto.NotifySlotTransOut{
				Slot:  int32(tst.Slot),
				Store: int32(tst.StoreTransferOut),
			})
		}
	} else {
		setIn := tst.pd.deployment.sets[tst.SetIn]
		for _, v := range setIn.nodes {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.host, v.interPort))
			tst.pd.udp.SendTo(addr, &sproto.NotifySlotTransIn{
				Slot:  int32(tst.Slot),
				Store: int32(tst.StoreTransferIn),
			})
		}
	}
	tst.timer = time.AfterFunc(time.Second*3, func() {
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
			st := p.pd.deployment.sets[t.SetOut].stores[t.StoreTransferOut]
			st.slots.Clear(int(t.Slot))
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
		st := p.deployment.sets[t.SetOut].stores[t.StoreTransferOut]
		st.slots.Clear(int(t.Slot))
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
		st := p.pd.deployment.sets[t.SetOut].stores[t.StoreTransferOut]
		st.slots.Set(int(t.Slot))
		if nil != t.timer {
			t.timer.Stop()
		}
	}
}

func (p *pd) replayNotifySlotTransInResp(reader *buffer.BufferReader) error {
	slot := int(reader.GetInt32())
	if t, ok := p.slotTransfer[slot]; ok {
		delete(p.slotTransfer, slot)
		st := p.deployment.sets[t.SetOut].stores[t.StoreTransferOut]
		st.slots.Set(int(t.Slot))
	}
	return nil
}

func (p *pd) beginSlotTransfer(slot int, setOut int, storeOut int, setIn int, storeIn int) error {
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
