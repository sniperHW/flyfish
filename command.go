package flyfish

import (
	"flyfish/errcode"
	protocol "flyfish/proto"
	"sync/atomic"
	"time"
)

const (
	cmdNone            = 0
	cmdGet             = 1
	cmdSet             = 2
	cmdSetNx           = 3
	cmdCompareAndSet   = 4
	cmdCompareAndSetNx = 5
	cmdDel             = 6
	cmdIncrBy          = 7
	cmdDecrBy          = 8
)

var (
	cmdCount int32 //待回复命令数量
)

func isSetCmd(cmd int) bool {
	return cmd == cmdSet || cmd == cmdSetNx || cmd == cmdCompareAndSet || cmd == cmdCompareAndSetNx || cmd == cmdIncrBy || cmd == cmdDecrBy
}

//命令回复器
type replyer interface {
	reply(errCode int32, fields map[string]*protocol.Field, version int64)
}

type cnsSt struct {
	oldV *protocol.Field
	newV *protocol.Field
}

//来自客户端的一条命令请求
type command struct {
	next     *command
	cmdType  int
	rpyer    replyer
	table    string
	key      string
	uniKey   string //table+key
	version  *int64
	ckey     *cacheKey
	fields   map[string]*protocol.Field //for get/set
	cns      *cnsSt                     //for compareAndSet
	incrDecr *protocol.Field            //for incr/decr
	deadline time.Time
}

func (this *command) reply(errCode int32, fields map[string]*protocol.Field, version int64) {
	this.rpyer.reply(errCode, fields, version)
	atomic.AddInt32(&cmdCount, -1)
}

func pushCommand(cmd *command) {

	atomic.AddInt32(&cmdCount, 1)

	meta := getMetaByTable(cmd.table)

	if nil == meta {
		cmd.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	if cmd.cmdType == cmdGet && !meta.checkGet(cmd.fields) {
		cmd.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (cmd.cmdType == cmdCompareAndSet || cmd.cmdType == cmdCompareAndSetNx) &&
		!meta.checkCompareAndSet(cmd.cns.newV, cmd.cns.oldV) {
		cmd.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (cmd.cmdType == cmdSet || cmd.cmdType == cmdSetNx) && !meta.checkSet(cmd.fields) {
		cmd.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (cmd.cmdType == cmdIncrBy || cmd.cmdType == cmdDecrBy) && !meta.checkField(cmd.incrDecr) {
		cmd.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	postKeyEvent(cmd.uniKey, processCmd, cmd)

}

func processCmd(cmds []interface{}) {
	cmd := cmds[0].(*command)
	cmd.ckey = getCacheKey(cmd.table, cmd.uniKey)
	cmd.ckey.pushCmd(cmd)
	cmd.ckey.process()
}

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			Infoln("redisReqCount", redisReqCount, "cmdCount", cmdCount)
		}
	}()
}
