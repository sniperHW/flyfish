package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type cmdI interface {
	getSeqno() int64
	reply(err errcode.Error, fields map[string]*flyproto.Field, version int64)
	isTimeout() bool
	cmdType() flyproto.CmdType
	do(*kvProposal)
}

type MakeResponse func(errcode.Error, map[string]*flyproto.Field, int64) *cs.RespMessage

type cmdBase struct {
	seqno           int64
	version         *int64
	peer            *net.Socket
	deadline        time.Time
	replied         int32
	wait4ReplyCount *int32
	fnMakeResponse  MakeResponse
	kv              *kv
	meta            db.TableMeta
}

func (this *cmdBase) init(kv *kv, peer *net.Socket, seqno int64, version *int64, deadline time.Time, wait4ReplyCount *int32, makeResponse MakeResponse) {
	atomic.AddInt32(wait4ReplyCount, 1)
	this.peer = peer
	this.deadline = deadline
	this.version = version
	this.seqno = seqno
	this.wait4ReplyCount = wait4ReplyCount
	this.fnMakeResponse = makeResponse
	this.kv = kv
	this.meta = kv.meta
}

func (this *cmdBase) getSeqno() int64 {
	return this.seqno
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
		atomic.AddInt32(this.wait4ReplyCount, -1)
		if nil != this.peer {
			if !time.Now().After(this.deadline) {
				resp := this.fnMakeResponse(err, fields, version)
				e := this.peer.Send(resp)
				if nil != e {
					GetSugar().Debugf("send resp error:%v", e)
				}
			}
		}
	}
}
