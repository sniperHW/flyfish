package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/list"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type cmdI interface {
	reply(err errcode.Error, fields map[string]*flyproto.Field, version int64)
	isTimeout() bool
	cmdType() flyproto.CmdType
	getListElement() *list.Element
}

type MakeResponse func(errcode.Error, map[string]*flyproto.Field, int64) *cs.RespMessage

type cmdBase struct {
	listElement    list.Element
	replyer        *replyer
	deadline       time.Time
	replied        int32
	fnMakeResponse MakeResponse
	kv             *kv
	meta           db.TableMeta
}

func (this *cmdBase) init(cmd interface{}, kv *kv, replyer *replyer, deadline time.Time, makeResponse MakeResponse) {
	this.replyer = replyer
	this.deadline = deadline
	this.fnMakeResponse = makeResponse
	this.kv = kv
	this.meta = kv.meta
	this.listElement.Value = cmd
}

func (this *cmdBase) getListElement() *list.Element {
	return &this.listElement
}

func (this *cmdBase) isTimeout() bool {
	if this.deadline.IsZero() {
		return false
	} else {
		return time.Now().After(this.deadline)
	}
}

func (this *cmdBase) reply(err errcode.Error, fields map[string]*flyproto.Field, version int64) {
	if atomic.CompareAndSwapInt32(&this.replied, 0, 1) {
		if nil != this.replyer {
			if !time.Now().After(this.deadline) {
				this.replyer.reply(this.fnMakeResponse(err, fields, version))
			} else {
				this.replyer.dropReply()
			}
		}
	}
}
