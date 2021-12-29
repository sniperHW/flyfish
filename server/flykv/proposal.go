package flykv

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flyproto "github.com/sniperHW/flyfish/proto"
	"math"
	"time"
	"unsafe"
)

const (
	proposal_none          = proposalType(0)
	proposal_snapshot      = proposalType(1) //全量数据kv快照,
	proposal_update        = proposalType(2) //fields变更
	proposal_kick          = proposalType(3) //从缓存移除kv
	proposal_lease         = proposalType(4) //数据库update权租约
	proposal_slots         = proposalType(5)
	proposal_slot_transfer = proposalType(6)
	proposal_meta          = proposalType(7)
	proposal_nop           = proposalType(8) //空proposal用于确保之前的proposal已经提交并apply
)

func newProposalReader(b []byte) proposalReader {
	return proposalReader{
		reader: buffer.NewReader(b),
	}
}

func (this *proposalReader) readField() (*flyproto.Field, error) {
	var err error
	var lname uint16
	var name string
	var tt byte
	lname, err = this.reader.CheckGetUint16()
	if nil != err {
		return nil, err
	}
	name, err = this.reader.CheckGetString(int(lname))
	if nil != err {
		return nil, err
	}

	tt, err = this.reader.CheckGetByte()
	if nil != err {
		return nil, err
	}
	switch flyproto.ValueType(tt) {
	case flyproto.ValueType_int, flyproto.ValueType_float:
		var i int64
		i, err = this.reader.CheckGetInt64()
		if nil != err {
			return nil, err
		} else if flyproto.ValueType(tt) == flyproto.ValueType_int {
			return flyproto.PackField(name, i), nil
		} else {
			return flyproto.PackField(name, math.Float64frombits(uint64(i))), nil
		}
	case flyproto.ValueType_string, flyproto.ValueType_blob:
		var l int32
		var b []byte
		l, err = this.reader.CheckGetInt32()
		if nil != err {
			return nil, err
		}
		b, err = this.reader.CopyBytes(int(l))
		if nil != err {
			return nil, err
		}
		if flyproto.ValueType(tt) == flyproto.ValueType_blob {
			return flyproto.PackField(name, b), nil
		} else {
			return flyproto.PackField(name, *(*string)(unsafe.Pointer(&b))), nil
		}
	default:
	}
	return nil, errors.New("bad data 1")
}

func (this *proposalReader) read() (isOver bool, ptype proposalType, data interface{}, err error) {
	if this.reader.IsOver() {
		isOver = true
		return
	} else {
		var b byte
		b, err = this.reader.CheckGetByte()
		if nil == err {
			ptype = proposalType(b)
			switch ptype {
			case proposal_nop:
				return
			case proposal_meta:
				var l int32
				l, err = this.reader.CheckGetInt32()
				if nil != err {
					err = fmt.Errorf("proposal_meta CheckGetInt32:%v", err)
					return
				}
				var bb []byte
				bb, err = this.reader.CheckGetBytes(int(l))
				if nil != err {
					err = fmt.Errorf("proposal_meta CheckGetBytes:%v", err)
					return
				}
				var meta db.DBMeta
				meta, err = sql.CreateDbMetaFromJson(bb)

				if nil != err {
					err = fmt.Errorf("proposal_meta CreateDbMetaFromJson:%v", err)
					return
				}

				data = meta

			case proposal_slots:
				var l int32
				l, err = this.reader.CheckGetInt32()
				if nil != err {
					err = fmt.Errorf("proposal_slots CheckGetInt32:%v", err)
					return
				}
				var bb []byte
				bb, err = this.reader.CheckGetBytes(int(l))
				if nil != err {
					err = fmt.Errorf("proposal_slots CheckGetBytes:%v", err)
					return
				}
				var slots *bitmap.Bitmap
				slots, err = bitmap.CreateFromJson(bb)
				if nil != err {
					err = fmt.Errorf("proposal_slots CreateFromJson:%v", err)
				} else {
					data = slots
				}
			case proposal_slot_transfer:
				var tt byte
				tt, err = this.reader.CheckGetByte()
				if nil != err {
					return
				}

				var slot int32
				slot, err = this.reader.CheckGetInt32()
				if nil != err {
					return
				}

				data = &SlotTransferProposal{
					slot:         int(slot),
					transferType: slotTransferType(tt),
				}
				return

			case proposal_none:
				err = errors.New("bad data 2")
			case proposal_lease:
				var id int32
				id, err = this.reader.CheckGetInt32()
				if nil != err {
					err = fmt.Errorf("proposal_lease read id:%v", err)
					return
				}
				var l int32
				l, err = this.reader.CheckGetInt32()
				if nil != err {
					err = fmt.Errorf("proposal_lease read l:%v", err)
					return
				}
				var bb []byte
				bb, err = this.reader.CheckGetBytes(int(l))
				if nil != err {
					err = fmt.Errorf("proposal_lease CheckGetBytes:%v", err)
					return
				}

				var t time.Time
				err = t.UnmarshalBinary(bb)
				if nil != err {
					err = fmt.Errorf("proposal_lease UnmarshalBinary:%v", err)
					return
				}

				data = pplease{
					nodeid:  int(id),
					begtime: t,
				}
				return
			case proposal_snapshot, proposal_update, proposal_kick:
				var l uint16
				l, err = this.reader.CheckGetUint16()
				if nil != err {
					return
				}
				p := ppkv{}
				p.unikey, err = this.reader.CheckGetString(int(l))
				if nil != err {
					return
				}

				if ptype != proposal_kick {

					p.version, err = this.reader.CheckGetInt64()
					if nil != err {
						return
					}
					var fieldSize int32
					fieldSize, err = this.reader.CheckGetInt32()
					if nil != err {
						return
					}

					var fields map[string]*flyproto.Field

					if fieldSize > 0 {
						fields = map[string]*flyproto.Field{}
						var field *flyproto.Field
						for i := int32(0); i < fieldSize; i++ {
							field, err = this.readField()
							if nil != err {
								return
							} else {
								fields[field.GetName()] = field
							}
						}
					}

					p.fields = fields
				}

				data = p
			default:
				err = errors.New("bad data 3")
			}
		}
		return
	}
}

type proposalBase struct {
}

func (this *proposalBase) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		c := getCompressor()
		cb, err := c.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
		releaseCompressor(c)
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
}

type kvProposal struct {
	proposalBase
	dbstate db.DBState
	ptype   proposalType
	fields  map[string]*flyproto.Field
	version int64
	cmds    []cmdI
	kv      *kv
}

type kvLinearizableRead struct {
	kv   *kv
	cmds []cmdI
}

func (this *kvProposal) Isurgent() bool {
	return false
}

func (this *kvProposal) OnError(err error) {

	GetSugar().Infof("kvProposal OnError:%v", err)

	for _, v := range this.cmds {
		v.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		if this.ptype == proposal_kick {
			this.kv.kicking = false
		}
		if this.kv.state == kv_loading {
			this.kv.store.deleteKv(this.kv)
			for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
				f.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
				this.kv.pendingCmd.popFront()
			}
		} else {
			this.kv.processPendingCmd()
		}
	})
}

func (this *kvProposal) Serilize(b []byte) []byte {
	return serilizeKv(b, this.ptype, this.kv.uniKey, this.version, this.fields)
}

func (this *kvProposal) apply() {
	if this.ptype == proposal_kick {
		for _, v := range this.cmds {
			v.reply(nil, nil, 0)
		}

		for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
			f.reply(errcode.New(errcode.Errcode_retry, "please try again"), nil, 0)
			this.kv.pendingCmd.popFront()
		}
		this.kv.store.deleteKv(this.kv)
	} else {

		oldState := this.kv.state

		if this.version == 0 {
			this.kv.state = kv_no_record
			this.kv.fields = nil
		} else {
			this.kv.state = kv_ok
		}

		this.kv.version = this.version
		if len(this.fields) > 0 {
			if nil == this.kv.fields {
				this.kv.fields = map[string]*flyproto.Field{}
			}
			for _, v := range this.fields {
				this.kv.fields[v.GetName()] = v
			}
		}

		for _, v := range this.cmds {
			v.reply(nil, this.kv.fields, this.version)
		}

		//update dbUpdateTask
		if this.dbstate != db.DBState_none {
			err := this.kv.updateTask.updateState(this.dbstate, this.version, this.fields)
			if nil != err {
				GetSugar().Errorf("%s updateState error:%v", this.kv.uniKey, err)
			}
		}

		if oldState == kv_loading {
			this.kv.store.lru.update(&this.kv.lru)
		}

		this.kv.processPendingCmd()

	}
}

func (this *kvLinearizableRead) OnError(err error) {

	GetSugar().Errorf("kvLinearizableRead OnError:%v", err)

	for _, v := range this.cmds {
		GetSugar().Infof("reply retry")
		v.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.processPendingCmd()
	})
}

func (this *kvLinearizableRead) ok() {
	GetSugar().Debugf("kvLinearizableRead ok:%d version:%d", len(this.cmds), this.kv.version)

	for _, v := range this.cmds {
		v.reply(nil, this.kv.fields, this.kv.version)
	}

	this.kv.processPendingCmd()
}

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	isPromote      bool
	url            string //for add
	nodeID         uint64
	reply          func(error)
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this *ProposalConfChange) GetURL() string {
	return this.url
}

func (this *ProposalConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this *ProposalConfChange) IsPromote() bool {
	return this.isPromote
}

func (this *ProposalConfChange) OnError(err error) {
	this.reply(err)
}

type ProposalUpdateMeta struct {
	proposalBase
	meta  db.DBMeta
	store *kvstore
	reply func()
}

func (this *ProposalUpdateMeta) Isurgent() bool {
	return true
}

func (this *ProposalUpdateMeta) OnError(err error) {
	GetSugar().Errorf("ProposalUpdateMeta error:%v", err)
}

func (this *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeMeta(this.meta, b)

}

func (this *ProposalUpdateMeta) apply() {
	this.meta.MoveTo(this.store.meta)
	this.reply()
}

type proposalNop struct {
	proposalBase
	store *kvstore
}

func (this *proposalNop) Isurgent() bool {
	return true
}

func (this *proposalNop) OnError(err error) {
	GetSugar().Errorf("proposalNop error:%v", err)
}

func (this *proposalNop) Serilize(b []byte) []byte {
	return buffer.AppendByte(b, byte(proposal_nop))

}

func (this *proposalNop) apply() {
	this.store.ready = true
}

type slotTransferType byte

const (
	slotTransferOut = slotTransferType(1)
	slotTransferIn  = slotTransferType(2)
)

type SlotTransferProposal struct {
	proposalBase
	slot         int
	transferType slotTransferType
	store        *kvstore
	reply        func()
	timer        *time.Timer
}

func (this *SlotTransferProposal) Isurgent() bool {
	return true
}

func (this *SlotTransferProposal) OnError(err error) {
	this.store.mainQueue.AppendHighestPriotiryItem(func() {
		delete(this.store.slotsTransferOut, this.slot)
	})
}

func (this *SlotTransferProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_slot_transfer))
	b = buffer.AppendByte(b, byte(this.transferType))
	return buffer.AppendInt32(b, int32(this.slot))
}

func (this *SlotTransferProposal) apply() {
	if this.transferType == slotTransferIn {
		this.store.slots.Set(this.slot)
		this.reply()
	} else if this.transferType == slotTransferOut {
		if nil == this.store.slotsKvMap[this.slot] {
			delete(this.store.slotsTransferOut, this.slot)
			this.store.slots.Set(this.slot)
			this.reply()
		} else {
			this.store.processSlotTransferOut(this)
		}
	}
}
