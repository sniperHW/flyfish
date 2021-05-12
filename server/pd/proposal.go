package pd

import (
	"encoding/json"
	"errors"
	//"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/timer"
	pdproto "github.com/sniperHW/flyfish/server/pd/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"time"
)

const (
	proposalAddKvnode            = 1
	proposalRemKvnode            = 2
	proposalAddStore             = 3
	proposalRemStore             = 4
	proposalKvnodeStoreTrans     = 5
	proposalKvnodeStoreTransResp = 6
	proposalSlotTransferPrepare  = 7
	proposalSlotTransferCancel   = 8
	proposalSlotTransferCommit   = 9
	proposalInitPd               = 10
)

const SlotTransferPrepareTimeout = 15 * time.Second

type proposalBase struct {
	pd    *pd
	reply func(...error)
}

type applyable interface {
	apply()
}

func (p *proposalBase) Isurgent() bool {
	return true
}

func (p *proposalBase) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
}

func (p *proposalBase) OnMergeFinish(b []byte) []byte {
	return b
}

type initPd struct {
	Kvnodes []jsonKvnode
	Stores  []jsonStore
}

type initPdProposal struct {
	*proposalBase
	initPd initPd
}

func (p *initPdProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalInitPd))
	bb, err := json.Marshal(p.initPd)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *initPdProposal) apply() {
	var err error
	for _, v := range p.initPd.Stores {
		s := &store{
			id:      v.Id,
			kvnodes: map[int]*kvnode{},
		}

		s.slots, err = bitmap.CreateFromJson(v.Slots)
		if nil != err {
			panic(err)
		}

		slots := s.slots.GetOpenBits()
		for _, vv := range slots {
			p.pd.slot2store[vv] = s
		}

		p.pd.stores[s.id] = s
	}

	for _, v := range p.initPd.Kvnodes {
		n := &kvnode{
			id:          v.Id,
			service:     v.Service,
			raftService: v.RaftService,
			udpService:  v.UdpService,
			stores:      map[int]*store{},
		}

		n.udpAddr, err = net.ResolveUDPAddr("udp", n.udpService)

		if nil != err {
			panic(err)
		}

		p.pd.kvnodes[v.Id] = n

		//GetSugar().Info(v.Stores)

		for _, vv := range v.Stores {
			s, ok := p.pd.stores[vv]
			if !ok {
				panic("store not found")
			} else {
				s.kvnodes[v.Id] = n
				n.stores[vv] = s
			}
		}
	}

	for _, v := range p.pd.stores {
		v.updateClusterStr()
	}

	GetSugar().Info("initPdProposal.apply")
}

type addKvnodeProposal struct {
	*proposalBase
	n *kvnode
}

func (p *addKvnodeProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddKvnode))
	j := jsonKvnode{
		Id:          p.n.id,
		Service:     p.n.service,
		RaftService: p.n.raftService,
		UdpService:  p.n.udpService,
	}

	for k, _ := range p.n.stores {
		j.Stores = append(j.Stores, k)
	}

	bb, err := json.Marshal(j)
	if nil != err {
		panic(err)
	}

	return buffer.AppendBytes(b, bb)
}

func (p *addKvnodeProposal) apply() {

	if nil == p.pd.kvnodes[p.n.id] {
		p.pd.kvnodes[p.n.id] = p.n
	}

	p.reply()
}

type remKvnodeProposal struct {
	*proposalBase
	n *kvnode
}

func (p *remKvnodeProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalRemKvnode))
	b = buffer.AppendUint32(b, uint32(p.n.id))
	return b
}

func (p *remKvnodeProposal) apply() {

	for _, v := range p.n.stores {
		delete(v.kvnodes, p.n.id)
	}

	delete(p.pd.kvnodes, p.n.id)
	p.reply()
}

type addStoreProposal struct {
	*proposalBase
	s int
}

func (p *addStoreProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddStore))
	b = buffer.AppendUint32(b, uint32(p.s))
	return b
}

func (p *addStoreProposal) apply() {
	p.pd.stores[p.s] = &store{
		id:      p.s,
		slots:   bitmap.New(slot.SlotCount),
		kvnodes: map[int]*kvnode{},
	}

	p.reply()
}

type remStoreProposal struct {
	*proposalBase
	s *store
}

func (p *remStoreProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalRemStore))
	b = buffer.AppendUint32(b, uint32(p.s.id))
	return b
}

func (p *remStoreProposal) apply() {
	if !p.s.removing {
		p.s.removing = true
		//请求pd将s上的slot迁移到别处,迁移完成后再删除s
	}
	p.reply()
}

type kvnodeStoreTransProposal struct {
	*proposalBase
	trans *nodeStoreTransaction
}

func (p *kvnodeStoreTransProposal) OnError(err error) {
	delete(p.pd.tmpTransNodeStore, p.trans.TransID)
	if nil != p.reply {
		p.reply(err)
	}
}

func (p *kvnodeStoreTransProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalKvnodeStoreTrans))
	buff, err := json.Marshal(p.trans)
	if nil != err {
		panic(err)
	}
	b = buffer.AppendBytes(b, buff)
	return b
}

func (p *kvnodeStoreTransProposal) apply() {
	delete(p.pd.tmpTransNodeStore, p.trans.TransID)
	p.pd.transNodeStore[p.trans.TransID] = p.trans
	p.trans.Notify()
	p.reply()

}

type kvnodeStoreTransRespProposal struct {
	*proposalBase
	nodeId   int
	isLeader int32
	trans    *nodeStoreTransaction
}

func (p *kvnodeStoreTransRespProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalKvnodeStoreTransResp))
	b = buffer.AppendInt64(b, p.trans.TransID)
	b = buffer.AppendUint32(b, uint32(p.nodeId))
	b = buffer.AppendInt32(b, p.isLeader)
	return b
}

func (p *kvnodeStoreTransRespProposal) apply() {
	if p.isLeader == 1 {
		p.trans.GotLeaderResp = true
	}

	if p.nodeId == p.trans.NodeId {
		p.trans.GotOtherResp = true
	}

	if p.trans.GotLeaderResp && p.trans.GotOtherResp {
		p.trans.timer.Cancel()
		p.trans.timer = nil
		//事务结束
		s := p.pd.stores[p.trans.StoreId]
		s.kvnodes[p.trans.NodeId] = p.pd.kvnodes[p.trans.NodeId]
		s.updateClusterStr()
		delete(p.pd.transNodeStore, p.trans.TransID)
	}
}

type slotTransferPrepareProposal struct {
	*proposalBase
	trans *slotTransferTransaction
}

func (p *slotTransferPrepareProposal) OnError(error error) {
	p.pd.mainque.AppendHighestPriotiryItem(func() {
		delete(p.pd.transferingSlot, p.trans.Slot)
	})
}

func (p *slotTransferPrepareProposal) Serilize(b []byte) []byte {

	b = buffer.AppendByte(b, byte(proposalSlotTransferPrepare))
	buff, err := json.Marshal(p.trans)
	if nil != err {
		panic(err)
	}
	b = buffer.AppendBytes(b, buff)
	return b
}

func (p *slotTransferPrepareProposal) apply() {
	p.pd.transSlotTransfer[p.trans.TransID] = p.trans

	prepare := &pdproto.SlotTransferPrepare{
		TransID:  proto.Int64(p.trans.TransID),
		Slot:     proto.Int32(int32(p.trans.Slot)),
		StoreIn:  proto.Int32(int32(p.trans.InStoreID)),
		StoreOut: proto.Int32(int32(p.trans.OutStoreID)),
	}

	for _, v := range p.pd.stores[p.trans.InStoreID].kvnodes {
		p.pd.udp.SendTo(v.udpAddr, prepare)
	}

	for _, v := range p.pd.stores[p.trans.OutStoreID].kvnodes {
		p.pd.udp.SendTo(v.udpAddr, prepare)
	}

	//启动定时器
	p.trans.timer = timer.New(SlotTransferPrepareTimeout, p.trans.onTransTimeout)

}

type slotTransferCancelProposal struct {
	*proposalBase
	trans *slotTransferTransaction
}

func (p *slotTransferCancelProposal) OnError(error error) {
	//如果自己还是leader再次触发proposal
	p.pd.mainque.AppendHighestPriotiryItem(func() {
		if p.pd.isLeader() {
			p.pd.issueProposal(p)
		}
	})
}

func (p *slotTransferCancelProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalSlotTransferCancel))
	b = buffer.AppendInt64(b, p.trans.TransID)
	return b
}

func (p *slotTransferCancelProposal) OnMergeFinish(b []byte) []byte {
	return b
}

func (p *slotTransferCancelProposal) apply() {
	p.trans.State = slotTransferCancel
	delete(p.pd.transferingSlot, p.trans.Slot)
	p.trans.notifyCancel()
}

type slotTransferCommitProposal struct {
	*proposalBase
	trans *slotTransferTransaction
}

func (p *slotTransferCommitProposal) OnError(error error) {
	//如果自己还是leader再次触发proposal
	p.pd.mainque.AppendHighestPriotiryItem(func() {
		if p.pd.isLeader() {
			p.pd.issueProposal(p)
		}
	})
}

func (p *slotTransferCommitProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalSlotTransferCommit))
	b = buffer.AppendInt64(b, p.trans.TransID)
	return b
}

func (p *slotTransferCommitProposal) apply() {
	delete(p.pd.transferingSlot, p.trans.Slot)
	p.trans.State = slotTransferCommit
	p.pd.stores[p.trans.InStoreID].slots.Set(p.trans.Slot)
	p.pd.stores[p.trans.OutStoreID].slots.Clear(p.trans.Slot)

	p.trans.notifyCommit()
}

//proposal回放

func (p *pd) replayInitPd(reader *buffer.BufferReader) error {
	i := &initPd{}
	var err error
	if err = json.Unmarshal(reader.GetAll(), &i); nil != err {
		return err
	}

	for _, v := range i.Stores {
		s := &store{
			id:      v.Id,
			kvnodes: map[int]*kvnode{},
		}
		var err error
		s.slots, err = bitmap.CreateFromJson(v.Slots)
		if nil != err {
			return err
		}

		slots := s.slots.GetOpenBits()
		for _, vv := range slots {
			p.slot2store[vv] = s
		}

		p.stores[s.id] = s
	}

	for _, v := range i.Kvnodes {
		n := &kvnode{
			id:          v.Id,
			service:     v.Service,
			raftService: v.RaftService,
			udpService:  v.UdpService,
			stores:      map[int]*store{},
		}

		n.udpAddr, err = net.ResolveUDPAddr("udp", n.udpService)

		if nil != err {
			return err
		}

		p.kvnodes[v.Id] = n

		for _, vv := range v.Stores {
			s, ok := p.stores[vv]
			if !ok {
				return errors.New("store not found")
			} else {
				s.kvnodes[v.Id] = n
				n.stores[vv] = s
			}
		}

	}

	for _, v := range p.stores {
		v.updateClusterStr()
	}

	GetSugar().Info("replayInitPd")

	return nil
}

func (p *pd) replayAddKvnode(reader *buffer.BufferReader) error {
	var err error

	j := jsonKvnode{}
	if err = json.Unmarshal(reader.GetAll(), &j); nil != err {
		return err
	}

	n, ok := p.kvnodes[j.Id]
	if ok {
		return errors.New("replayAddKvnode error 1")
	}

	n = &kvnode{
		id:          j.Id,
		service:     j.Service,
		raftService: j.RaftService,
		stores:      map[int]*store{},
		udpService:  j.UdpService,
	}

	if n.udpAddr, err = net.ResolveUDPAddr("udp", j.UdpService); nil != err {
		return errors.New("replayAddKvnode error 2")
	}

	for _, v := range j.Stores {
		s, ok := p.stores[v]
		if !ok {
			return errors.New("replayAddKvnode error 3")
		}
		n.stores[v] = s
		s.kvnodes[n.id] = n
	}

	p.kvnodes[n.id] = n

	return nil
}

func (p *pd) replayRemKvnode(reader *buffer.BufferReader) error {
	if id, err := reader.CheckGetInt32(); nil != err {
		return errors.New("replayRemKvnode error 1")
	} else {
		n, ok := p.kvnodes[int(id)]
		if !ok {
			return errors.New("replayRemKvnode error 2")
		}

		for _, v := range n.stores {
			if nil == v.kvnodes[n.id] {
				return errors.New("replayRemKvnode error 3")
			}
			delete(v.kvnodes, n.id)
		}

		delete(p.kvnodes, n.id)
	}

	return nil
}

func (p *pd) replayAddStore(reader *buffer.BufferReader) error {
	if id, err := reader.CheckGetInt32(); nil != err {
		return errors.New("replayAddStore error 1")
	} else {
		_, ok := p.stores[int(id)]
		if ok {
			return errors.New("replayAddStore error 2")
		}
		p.stores[int(id)] = &store{
			id:      int(id),
			slots:   bitmap.New(slot.SlotCount),
			kvnodes: map[int]*kvnode{},
		}
	}
	return nil
}

func (p *pd) replayRemStore(reader *buffer.BufferReader) error {
	if id, err := reader.CheckGetInt32(); nil != err {
		return errors.New("replayRemStore error 1")
	} else {
		_, ok := p.stores[int(id)]
		if !ok {
			return errors.New("replayRemStore error 2")
		}
		delete(p.stores, int(id))
	}
	return nil
}

func (p *pd) replayKvnodeStoreTrans(reader *buffer.BufferReader) error {
	trans := nodeStoreTransaction{}
	var err error
	if err = json.Unmarshal(reader.GetAll(), &trans); nil != err {
		return err
	}
	trans.pd = p
	p.transNodeStore[trans.TransID] = &trans
	return nil
}

func (p *pd) replayKvnodeStoreTransResp(reader *buffer.BufferReader) error {
	var err error
	transID, err := reader.CheckGetInt64()
	if nil != err {
		return errors.New("replayKvnodeStoreTransResp error 1")
	}

	trans, ok := p.transNodeStore[transID]
	if !ok {
		return errors.New("replayKvnodeStoreTransResp error 2")
	}

	nodeId, err := reader.CheckGetInt32()
	if nil != err {
		return errors.New("replayKvnodeStoreTransResp error 3")
	}

	isLeader, err := reader.CheckGetInt32()
	if nil != err {
		return errors.New("replayKvnodeStoreTransResp error 4")
	}

	_, ok = p.kvnodes[int(nodeId)]
	if !ok {
		return errors.New("replayKvnodeStoreTransResp error 5")
	}

	if isLeader == 1 {
		trans.GotLeaderResp = true
	}

	if int(nodeId) == trans.NodeId {
		trans.GotOtherResp = true
	}

	if trans.GotLeaderResp && trans.GotOtherResp {
		//事务结束
		s := p.stores[trans.StoreId]
		if nil == s {
			return errors.New("replayKvnodeStoreTransResp error 6")
		}

		s.kvnodes[trans.NodeId] = p.kvnodes[trans.NodeId]
		s.updateClusterStr()
		delete(p.transNodeStore, trans.TransID)
	}

	return nil
}

func (p *pd) replaySlotTransferPrepare(reader *buffer.BufferReader) error {
	trans := slotTransferTransaction{}
	var err error
	if err = json.Unmarshal(reader.GetAll(), &trans); nil != err {
		return err
	}

	if nil != p.transSlotTransfer[trans.TransID] {
		return errors.New("replaySlotTransferPrepare error 1")
	}

	trans.pd = p
	p.transSlotTransfer[trans.TransID] = &trans
	return nil
}

func (p *pd) replaySlotTransferCancel(reader *buffer.BufferReader) error {
	var err error
	transID, err := reader.CheckGetInt64()
	if nil != err {
		return errors.New("replaySlotTransferCancel error 1")
	}

	trans, ok := p.transSlotTransfer[transID]
	if !ok {
		return errors.New("replaySlotTransferCancel error 2")
	}

	trans.State = slotTransferCancel

	return nil
}

func (p *pd) replaySlotTransferCommit(reader *buffer.BufferReader) error {
	var err error
	transID, err := reader.CheckGetInt64()
	if nil != err {
		return errors.New("replaySlotTransferCommit error 1")
	}

	trans, ok := p.transSlotTransfer[transID]
	if !ok {
		return errors.New("replaySlotTransferCommit error 2")
	}

	trans.State = slotTransferCommit

	return nil
}

func (p *pd) replayProposal(proposal []byte) error {
	reader := buffer.NewReader(proposal)
	proposalType, err := reader.CheckGetByte()
	if nil != err {
		return err
	}

	switch int(proposalType) {
	case proposalInitPd:
		return p.replayInitPd(&reader)
	case proposalAddKvnode:
		return p.replayAddKvnode(&reader)
	case proposalRemKvnode:
		return p.replayRemKvnode(&reader)
	case proposalAddStore:
		return p.replayAddStore(&reader)
	case proposalRemStore:
		return p.replayRemStore(&reader)
	case proposalKvnodeStoreTrans:
		return p.replayKvnodeStoreTrans(&reader)
	case proposalKvnodeStoreTransResp:
		return p.replayKvnodeStoreTransResp(&reader)
	case proposalSlotTransferPrepare:
		return p.replaySlotTransferPrepare(&reader)
	case proposalSlotTransferCancel:
		return p.replaySlotTransferCancel(&reader)
	case proposalSlotTransferCommit:
		return p.replaySlotTransferCommit(&reader)
	default:
		return errors.New("invaild proposal type")
	}

	return nil
}
