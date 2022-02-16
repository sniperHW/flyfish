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
	dropReply()
	cmdType() flyproto.CmdType
	onLoadResult(err error, proposal *kvProposal) //call when err==nil or err==ERR_RecordNotExist
	versionMatch(*kv) bool
	getNext() cmdI
	setNext(cmdI)
}

type MakeResponse func(errcode.Error, map[string]*flyproto.Field, int64) *cs.RespMessage

type cmdBase struct {
	cmd             flyproto.CmdType
	seqno           int64
	version         *int64
	peer            *net.Socket
	processDeadline time.Time
	respDeadline    time.Time
	replied         int32
	wait4ReplyCount *int32
	fnMakeResponse  MakeResponse
	ppnext          cmdI
	kv              *kv
	meta            db.TableMeta
}

func (this *cmdBase) init(kv *kv, cmd flyproto.CmdType, peer *net.Socket, seqno int64, version *int64, processDeadline time.Time, respDeadline time.Time, wait4ReplyCount *int32, makeResponse MakeResponse) {
	atomic.AddInt32(wait4ReplyCount, 1)
	this.peer = peer
	this.respDeadline = respDeadline
	this.processDeadline = processDeadline
	this.version = version
	this.seqno = seqno
	this.cmd = cmd
	this.wait4ReplyCount = wait4ReplyCount
	this.fnMakeResponse = makeResponse
	this.kv = kv
	this.meta = kv.meta
}

func (this *cmdBase) getNext() cmdI {
	return this.ppnext
}

func (this *cmdBase) setNext(n cmdI) {
	this.ppnext = n
}

func (this *cmdBase) cmdType() flyproto.CmdType {
	return this.cmd
}

func (this *cmdBase) getSeqno() int64 {
	return this.seqno
}

func (this *cmdBase) isTimeout() bool {
	if this.processDeadline.IsZero() {
		return false
	} else {
		return time.Now().After(this.processDeadline)
	}
}

func (this *cmdBase) reply(err errcode.Error, fields map[string]*flyproto.Field, version int64) {
	if atomic.CompareAndSwapInt32(&this.replied, 0, 1) {
		atomic.AddInt32(this.wait4ReplyCount, -1)
		if nil != this.peer {
			if !time.Now().After(this.respDeadline) {
				resp := this.fnMakeResponse(err, fields, version)
				e := this.peer.Send(resp)
				if nil != e {
					GetSugar().Debugf("send resp error:%v", e)
				}
			}
		}
	}
}

func (this *cmdBase) dropReply() {
	if atomic.CompareAndSwapInt32(&this.replied, 0, 1) {
		atomic.AddInt32(this.wait4ReplyCount, -1)
	}
}

func (this *cmdBase) versionMatch(kv *kv) bool {
	if nil == this.version {
		return true
	} else if !(this.cmd == flyproto.CmdType_Get || this.cmd == flyproto.CmdType_Kick) && *this.version != kv.version {
		return false
	} else {
		return true
	}
}
