package kvnode

import (
	"fmt"
	//pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
	"time"
)

var (
	wait4ReplyCount int64
)

//func makeRespCommon(key string /*seqno int64, errCode int32,*/, version int64) *proto.RespCommon {
//	return &proto.RespCommon{
//		Key: key, //pb.String(key),
//Seqno:   seqno,   //pb.Int64(this.replyer.seqno),
//ErrCode: errCode, //pb.Int32(errCode),
//		Version: version, //pb.Int64(version),
//	}
//}

/*func checkReqCommon(reqCommon *proto.ReqCommon) int32 {
	if "" == reqCommon.GetTable() {
		return errcode.ERR_MISSING_TABLE
	}

	if "" == reqCommon.GetKey() {
		return errcode.ERR_MISSING_KEY
	}

	return errcode.ERR_OK
}*/

func makeUniKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

type commandI interface {
	makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message //pb.Message
	reply(errCode int32, fields map[string]*proto.Field, version int64)
	dontReply()
	isCancel() bool  //操作是否被取消(客户端连接断开或主动发送取消请求)
	getKV() *kv      //获取op操作的目标
	isTimeout() bool //命令已经超时
	checkVersion(version int64) bool
	prepare(asynCmdTaskI) (asynCmdTaskI, bool)
}

type commandBase struct {
	kv       *kv
	deadline time.Time
	replyer  *replyer
	version  *int64
}

func (this *commandBase) dontReply() {
	this.replyer.dontReply()
}

func (this *commandBase) isCancel() bool {
	return this.replyer.isCancel()
}

func (this *commandBase) getKV() *kv {
	return this.kv
}

func (this *commandBase) isTimeout() bool {
	return time.Now().After(this.deadline)
}

func (this *commandBase) checkVersion(version int64) bool {
	if this.version == nil {
		return true
	} else {
		return *this.version == version
	}
}

type replyer struct {
	replyed      int64
	seqno        int64
	peer         *cliConn
	respDeadline time.Time
}

func newReplyer(peer *cliConn, seqno int64, respDeadline time.Time) *replyer {
	atomic.AddInt64(&wait4ReplyCount, 1)

	r := &replyer{
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

func (this *replyer) reply(cmd commandI, errCode int32, fields map[string]*proto.Field, version int64) {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
		if this.peer.removeReplyer(this) && !time.Now().After(this.respDeadline) {
			err := this.peer.send(cmd.makeResponse(errCode, fields, version))
			if nil != err {
				Errorln("send resp error", err.Error())
			}
		}
	}
}

func (this *replyer) dontReply() {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		atomic.AddInt64(&wait4ReplyCount, -1)
		this.peer.removeReplyer(this)
	}
}
