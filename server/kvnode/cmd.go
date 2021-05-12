package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync"
	"sync/atomic"
	"time"
)

type replyAble interface {
	getSeqno() int64
	reply(err errcode.Error, fields map[string]*flyproto.Field, version int64)
	isCancel() bool
	isTimeout() bool
	dontReply()
}

type cmdI interface {
	replyAble
	cmdType() flyproto.CmdType
	onLoadResult(err error, proposal *kvProposal) //当err==nil或err==ERR_RecordNotExist才会调用
	checkVersion() bool
	check(keyvalue *kv) bool
}

type MakeResponse func(errcode.Error, map[string]*flyproto.Field, int64) *cs.RespMessage

type cmdBase struct {
	cmd             flyproto.CmdType
	seqno           int64
	version         *int64
	peer            *conn
	processDeadline time.Time
	respDeadline    time.Time
	replyOnce       sync.Once
	wait4ReplyCount *int32
	fnMakeResponse  MakeResponse
}

func initCmdBase(base *cmdBase, cmd flyproto.CmdType, peer *conn, seqno int64, version *int64, processDeadline time.Time, respDeadline time.Time, wait4ReplyCount *int32, makeResponse MakeResponse) {
	atomic.AddInt32(wait4ReplyCount, 1)
	base.peer = peer
	base.respDeadline = respDeadline
	base.processDeadline = processDeadline
	base.version = version
	base.seqno = seqno
	base.cmd = cmd
	base.wait4ReplyCount = wait4ReplyCount
	base.fnMakeResponse = makeResponse
	peer.addPendingCmd(base)
}

func (this *cmdBase) checkVersion() bool {
	return this.version != nil
}

func (this *cmdBase) cmdType() flyproto.CmdType {
	return this.cmd
}

func (this *cmdBase) getSeqno() int64 {
	return this.seqno
}

func (this *cmdBase) isCancel() bool {
	if this.peer.isClosed() {
		return true
	}

	if !this.peer.checkPendingCmd(this) {
		return true
	}

	return false
}

func (this *cmdBase) isTimeout() bool {
	return time.Now().After(this.processDeadline)
}

func (this *cmdBase) reply(err errcode.Error, fields map[string]*flyproto.Field, version int64) {
	this.replyOnce.Do(func() {
		atomic.AddInt32(this.wait4ReplyCount, -1)
		if this.peer.removePendingCmd(this) && !time.Now().After(this.respDeadline) {
			resp := this.fnMakeResponse(err, fields, version)
			e := this.peer.send(resp)
			if nil != e {
				GetSugar().Errorf("send resp error:%v", e)
			}
		}
	})
}

func (this *cmdBase) dontReply() {
	this.replyOnce.Do(func() {
		atomic.AddInt32(this.wait4ReplyCount, -1)
		this.peer.removePendingCmd(this)
	})
}

func (this *cmdBase) check(keyvalue *kv) bool {
	switch this.cmd {
	case flyproto.CmdType_Get:
	default:
		if nil != this.version && *this.version != keyvalue.version {
			this.reply(Err_version_mismatch, nil, 0)
			return false
		}
	}
	return true
}
