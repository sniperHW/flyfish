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

type replyer struct {
	replyed      int64
	session      kendynet.StreamSession
	respDeadline time.Time
}

func newReplyer(session kendynet.StreamSession, respDeadline time.Time) *replyer {
	atomic.AddInt64(&wait4ReplyCount, 1)
	return &replyer{
		session:      session,
		respDeadline: respDeadline,
	}
}

func (this *replyer) isClosed() bool {
	return this.session.IsClosed()
}

func (this *replyer) reply(op opI, errCode int32, fields map[string]*proto.Field, version int64) {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
		if !time.Now().After(this.respDeadline) {
			resp := op.makeResponse(errCode, fields, version)
			this.session.Send(resp)
		}
	}
}

func (this *replyer) dontReply() {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
	}
}

type opI interface {
	makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message
	reply(errCode int32, fields map[string]*proto.Field, version int64)
	dontReply()
	causeWriteBack() bool  //是否会导致回写
	isSetOp() bool         //是否设置类操作
	isReplyerClosed() bool //replyer是否已经关闭
	getKV() *kv            //获取op操作的目标
	isTimeout() bool       //命令已经超时
	//checkHead(*codec.Message) int32 //检查传递的参数是否合法
}

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
