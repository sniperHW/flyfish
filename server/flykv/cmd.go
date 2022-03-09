package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type cmdI interface {
	reply(err errcode.Error, fields map[string]*flyproto.Field, version int64)
	isTimeout() bool
	cmdType() flyproto.CmdType
}

type MakeResponse func(errcode.Error, map[string]*flyproto.Field, int64) *cs.RespMessage

type cmdBase struct {
	seqno          int64
	version        *int64
	replyer        *replyer
	deadline       time.Time
	replied        int32
	fnMakeResponse MakeResponse
	kv             *kv
	meta           db.TableMeta
}

func (this *cmdBase) init(kv *kv, replyer *replyer, seqno int64, version *int64, deadline time.Time, makeResponse MakeResponse) {
	this.replyer = replyer
	this.deadline = deadline
	this.version = version
	this.seqno = seqno
	this.fnMakeResponse = makeResponse
	this.kv = kv
	this.meta = kv.meta
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
