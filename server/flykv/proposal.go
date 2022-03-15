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
	"unsafe"
)

const (
	proposal_none                   = proposalType(0)
	proposal_snapshot               = proposalType(1) //全量数据kv快照,
	proposal_update                 = proposalType(2) //fields变更
	proposal_kick                   = proposalType(3) //从缓存移除kv
	proposal_slots                  = proposalType(4)
	proposal_slot_transfer          = proposalType(5)
	proposal_meta                   = proposalType(6)
	proposal_nop                    = proposalType(7) //空proposal用于确保之前的proposal已经提交并apply
	proposal_last_writeback_version = proposalType(8)
	proposal_suspend                = proposalType(9)
	proposal_resume                 = proposalType(10)
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
			case proposal_suspend:
				data = struct{}{}
			case proposal_resume:
				data = struct{}{}
			case proposal_nop:
				data = struct{}{}
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
			case proposal_none:
				err = errors.New("bad data 2")
			case proposal_last_writeback_version:
				var l uint16
				l, err = this.reader.CheckGetUint16()
				if nil != err {
					return
				}
				kv := &kv{}
				kv.uniKey, err = this.reader.CheckGetString(int(l))
				if nil != err {
					return
				}

				kv.lastWriteBackVersion, err = this.reader.CheckGetInt64()
				if nil != err {
					return
				}

				data = kv
			case proposal_snapshot, proposal_update, proposal_kick:
				var l uint16
				l, err = this.reader.CheckGetUint16()
				if nil != err {
					return
				}
				kv := &kv{}
				kv.uniKey, err = this.reader.CheckGetString(int(l))
				if nil != err {
					return
				}
				kv.version, err = this.reader.CheckGetInt64()
				if nil != err {
					return
				}

				kv.lastWriteBackVersion, err = this.reader.CheckGetInt64()
				if nil != err {
					return
				}

				if ptype != proposal_kick {

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

					kv.fields = fields
				}

				data = kv
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
	kvState     kvState
	ptype       proposalType
	fields      map[string]*flyproto.Field
	version     int64
	cmds        []cmdI
	kv          *kv
	dbversion   int64
	causeByLoad bool
}

type kvLinearizableRead struct {
	kv   *kv
	cmds []cmdI
}

func (this *kvProposal) reply(err errcode.Error, fields map[string]*flyproto.Field, version int64) {
	for _, v := range this.cmds {
		v.reply(err, fields, version)
	}
}

func (this *kvProposal) OnError(err error) {
	GetSugar().Errorf("kvProposal OnError:%v", err)
	this.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.clearCmds(errcode.New(errcode.Errcode_error, err.Error()))
		if this.kv.state == kv_loading {
			this.kv.store.deleteKv(this.kv)
		}
	})
}

func (this *kvProposal) Serilize(b []byte) []byte {
	return serilizeKv(b, this.ptype, this.kv.uniKey, this.version, this.dbversion, this.fields)
}

func (this *kvProposal) apply() {
	if this.ptype == proposal_kick {
		this.kv.store.deleteKv(this.kv)
		this.reply(nil, nil, 0)
	} else {

		this.kv.state = this.kvState
		if this.kv.state == kv_no_record {
			this.kv.fields = nil
		}

		if this.causeByLoad {
			this.kv.updateTask.setLastWriteBackVersion(this.dbversion)
			this.kv.lastWriteBackVersion = this.dbversion
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

		this.reply(nil, this.kv.fields, this.version)

		//update dbUpdateTask
		if !this.causeByLoad {
			var dbstate db.DBState
			if this.ptype == proposal_snapshot {
				if this.kv.state == kv_ok {
					dbstate = db.DBState_insert
				} else {
					if this.kv.lastWriteBackVersion == 0 {
						dbstate = db.DBState_insert
					} else {
						dbstate = db.DBState_delete
					}
				}
			} else {
				if this.kv.lastWriteBackVersion == 0 {
					dbstate = db.DBState_insert
				} else {
					dbstate = db.DBState_update
				}
			}

			if err := this.kv.updateTask.updateState(dbstate, this.version, this.fields); nil != err {
				GetSugar().Errorf("%s updateState error:%v", this.kv.uniKey, err)
			}
		}

		this.kv.processCmd()
	}
}

func (this *kvLinearizableRead) reply(err errcode.Error, fields map[string]*flyproto.Field, version int64) {
	for _, v := range this.cmds {
		v.reply(err, fields, version)
	}
}

func (this *kvLinearizableRead) OnError(err error) {
	GetSugar().Errorf("kvLinearizableRead OnError:%v", err)
	this.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.clearCmds(errcode.New(errcode.Errcode_error, err.Error()))
	})
}

func (this *kvLinearizableRead) ok() {
	GetSugar().Debugf("kvLinearizableRead ok version:%d", this.kv.version)
	this.reply(nil, this.kv.fields, this.kv.version)
	this.kv.processCmd()
}

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	isPromote      bool
	url            string //for add
	clientUrl      string
	nodeID         uint64
	processID      uint16
	reply          func(error)
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this *ProposalConfChange) GetClientURL() string {
	return this.clientUrl
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

func (this *ProposalConfChange) GetProcessID() uint16 {
	return this.processID
}

type ProposalUpdateMeta struct {
	proposalBase
	meta  db.DBMeta
	store *kvstore
}

func (this *ProposalUpdateMeta) OnError(err error) {
	GetSugar().Errorf("ProposalUpdateMeta error:%v", err)
}

func (this *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeMeta(this.meta, b)

}

func (this *ProposalUpdateMeta) apply() {
	this.meta.MoveTo(this.store.meta)
	GetSugar().Errorf("ProposalUpdateMeta apply:%v", this.store.meta.GetVersion())
}

type proposalNop struct {
	proposalBase
	store *kvstore
}

func (this *proposalNop) OnError(err error) {
	GetSugar().Errorf("proposalNop error:%v", err)
}

func (this *proposalNop) Serilize(b []byte) []byte {
	return buffer.AppendByte(b, byte(proposal_nop))
}

func (this *proposalNop) apply() {
	this.store.applyNop()
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
}

func (this *SlotTransferProposal) OnError(err error) {

}

func (this *SlotTransferProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_slot_transfer))
	b = buffer.AppendByte(b, byte(this.transferType))
	return buffer.AppendInt32(b, int32(this.slot))
}

func (this *SlotTransferProposal) apply() {
	if this.transferType == slotTransferIn {
		this.store.slots.Set(this.slot)
	} else {
		delete(this.store.slotsTransferOut, this.slot)
		this.store.slots.Clear(this.slot)
	}
	this.reply()
}

type LastWriteBackVersionProposal struct {
	proposalBase
	kv      *kv
	version int64
}

func (this *LastWriteBackVersionProposal) OnError(err error) {

}

func (this *LastWriteBackVersionProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_last_writeback_version))
	//unikey
	b = buffer.AppendUint16(b, uint16(len(this.kv.uniKey)))
	b = buffer.AppendString(b, this.kv.uniKey)
	return buffer.AppendInt64(b, this.version)
}

func (this *LastWriteBackVersionProposal) apply() {
	GetSugar().Debugf("LastWriteBackVersionProposal apply %s version:%d %d", this.kv.uniKey, this.version, this.kv.version)
	if abs(this.version) > abs(this.kv.lastWriteBackVersion) {
		this.kv.lastWriteBackVersion = this.version
	}

	if f := this.kv.pendingCmd.Front(); nil != f {
		if cmdkick, ok := f.Value.(*cmdKick); ok && this.version >= cmdkick.waitVersion {
			GetSugar().Debugf("LastWriteBackVersionProposal apply %s process pending kick", this.kv.uniKey)
			this.kv.processCmd()
		}
	}
}

type SuspendProposal struct {
	proposalBase
	store *kvstore
}

func (this *SuspendProposal) OnError(err error) {

}

func (this *SuspendProposal) Serilize(b []byte) []byte {
	return buffer.AppendByte(b, byte(proposal_suspend))
}

func (this *SuspendProposal) apply() {
	this.store.halt = true
}

type ResumeProposal struct {
	proposalBase
	store *kvstore
}

func (this *ResumeProposal) OnError(err error) {

}

func (this *ResumeProposal) Serilize(b []byte) []byte {
	return buffer.AppendByte(b, byte(proposal_resume))
}

func (this *ResumeProposal) apply() {
	this.store.halt = false
}
