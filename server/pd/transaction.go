package pd

import (
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/timer"
	pdproto "github.com/sniperHW/flyfish/server/pd/proto"
	"time"
)

func makeTransactionNodeStoreID(storeID int, nodeID int) int64 {
	return int64(storeID)<<32 + int64(nodeID)
}

//kvnode增/删store的事务
type nodeStoreTransaction struct {
	TransID       int64
	Type          pdproto.KvnodeStoreTransType
	NodeId        int
	StoreId       int
	GotLeaderResp bool
	GotOtherResp  bool

	pd    *pd
	timer *timer.Timer
}

//slot迁移事务
type slotTransferState int

const (
	slotTransferPrepare = slotTransferState(0)
	slotTransferCommit  = slotTransferState(1)
	slotTransferCancel  = slotTransferState(2)
)

type slotTransferTransaction struct {
	TransID    int64
	State      slotTransferState
	Slot       int //slot will be transfer
	OutStoreID int
	InStoreID  int

	tmpState slotTransferState
	inAgree  bool
	outAgree bool
	timer    *timer.Timer
	pd       *pd
}

func (t *slotTransferTransaction) isPrepare() bool {
	return t.State == slotTransferPrepare && t.tmpState == slotTransferPrepare
}

func (t *slotTransferTransaction) isCancel() bool {
	return t.State == slotTransferCancel || t.tmpState == slotTransferCancel
}

func (t *slotTransferTransaction) isCommit() bool {
	return t.State == slotTransferCommit
}

func (t *nodeStoreTransaction) Notify() {
	msg := &pdproto.NotifyKvnodeStoreTrans{
		TransID:   proto.Int64(t.TransID),
		TransType: pdproto.KvnodeStoreTransType.Enum(t.Type),
		NodeId:    proto.Int32(int32(t.NodeId)),
		StoreId:   proto.Int32(int32(t.StoreId)),
	}

	for _, v := range t.pd.stores[t.StoreId].kvnodes {
		t.pd.udp.SendTo(v.udpAddr, msg)
	}

	if t.Type == pdproto.KvnodeStoreTransType_TransAddStore {
		t.pd.udp.SendTo(t.pd.kvnodes[t.NodeId].udpAddr, msg)
	}

	t.timer = timer.New(time.Second, t.onTimeout)
}

func (t *nodeStoreTransaction) onTimeout(_ *timer.Timer, _ interface{}) {
	t.Notify()
}

func (trans *slotTransferTransaction) notifyCancel() {
	toIn := &pdproto.SlotTransferCancel{
		TransID: proto.Int64(trans.TransID),
		StoreID: proto.Int32(int32(trans.InStoreID)),
	}

	for _, v := range trans.pd.stores[trans.InStoreID].kvnodes {
		trans.pd.udp.SendTo(v.udpAddr, toIn)
	}

	toOut := &pdproto.SlotTransferCancel{
		TransID: proto.Int64(trans.TransID),
		StoreID: proto.Int32(int32(trans.OutStoreID)),
	}

	for _, v := range trans.pd.stores[trans.OutStoreID].kvnodes {
		trans.pd.udp.SendTo(v.udpAddr, toOut)
	}
}

func (trans *slotTransferTransaction) notifyCommit() {
	toIn := &pdproto.SlotTransferCommit{
		TransID: proto.Int64(trans.TransID),
		StoreID: proto.Int32(int32(trans.InStoreID)),
	}

	for _, v := range trans.pd.stores[trans.InStoreID].kvnodes {
		trans.pd.udp.SendTo(v.udpAddr, toIn)
	}

	toOut := &pdproto.SlotTransferCommit{
		TransID: proto.Int64(trans.TransID),
		StoreID: proto.Int32(int32(trans.OutStoreID)),
	}

	for _, v := range trans.pd.stores[trans.OutStoreID].kvnodes {
		trans.pd.udp.SendTo(v.udpAddr, toOut)
	}
}

func (trans *slotTransferTransaction) onTransTimeout(t *timer.Timer, ud interface{}) {
	if trans.isPrepare() {
		//定时器到期，没有收到全部确认
		trans.notifyCancel()
		trans.pd.issueProposal(&slotTransferCancelProposal{
			trans: trans,
			proposalBase: &proposalBase{
				pd: trans.pd,
			},
		})
	}
}
