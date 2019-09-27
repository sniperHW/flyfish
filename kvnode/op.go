package kvnode

import (
	"fmt"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	pb "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"sync/atomic"
	"time"
)

var (
	wait4ReplyCount int64
)

func checkReqCommon(reqCommon *proto.ReqCommon) int32 {
	if "" == reqCommon.GetTable() {
		return errcode.ERR_MISSING_TABLE
	}

	if "" == reqCommon.GetKey() {
		return errcode.ERR_MISSING_KEY
	}

	return errcode.ERR_OK
}

func makeUniKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

type opI interface {
	makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message
	reply(errCode int32, fields map[string]*proto.Field, version int64)
	dontReply()
	isCancel() bool
	getKV() *kv      //获取op操作的目标
	isTimeout() bool //命令已经超时
}

type opBase struct {
	kv       *kv
	deadline time.Time
	replyer  *replyer
}

func (this *opBase) dontReply() {
	this.replyer.dontReply()
}

func (this *opBase) isCancel() bool {
	this.replyer.isCancel()
}

func (this *opBase) getKV() *kv {
	return this.kv
}

func (this *opBase) isTimeout() bool {
	return time.Now().After(this.deadline)
}

type replyer struct {
	replyed      int64
	seqno        int64
	peer         *cliConn
	respDeadline time.Time
}

func newReplyer(peer *cliConn, seqno int64, respDeadline time.Time) *replyer {
	atomic.AddInt64(&wait4ReplyCount, 1)

	r = &replyer{
		peer:         peer,
		respDeadline: respDeadline,
		seqno:        seqno,
	}

	peer.addReplyer(r)

	return r
}

func (this *replyer) isCancel() bool {
	if this.peer.isClosed() {
		return true
	}

	if !this.peer.checkReplyer(this) {
		return true
	}

	return false
}

func (this *replyer) reply(op opI, errCode int32, fields map[string]*proto.Field, version int64) {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
		if this.peer.removeReplyer(this) && !time.Now().After(this.respDeadline) {
			resp := op.makeResponse(errCode, fields, version)
			this.peer.send(resp)
		}
	}
}

func (this *replyer) dontReply() {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
		this.peer.removeReplyer(this)
	}
}
