package sqlnode

import (
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type cmd interface {
	seqNo() int64
	uniKey() string
	canCombine() bool
	isProcessTimeout() bool
	makeSqlTask() sqlTask
	reply(errCode int32, version int64, fields map[string]*proto.Field)
	replyError(int32)
}

type cmdBase struct {
	conn             *cliConn
	sqNo             int64
	uKey             string
	table            string
	key              string
	processDeadline  time.Time
	responseDeadline time.Time
}

func newCmdBase(conn *cliConn, sqNo int64, unikey, table, key string, procDeadline, respDeadline time.Time) cmdBase {
	return cmdBase{
		conn:             conn,
		sqNo:             sqNo,
		uKey:             unikey,
		table:            table,
		key:              key,
		processDeadline:  procDeadline,
		responseDeadline: respDeadline,
	}
}

func (c *cmdBase) seqNo() int64 {
	return c.sqNo
}

func (c *cmdBase) uniKey() string {
	return c.uKey
}

func (c *cmdBase) isProcessTimeout() bool {
	return !c.processDeadline.After(time.Now())
}

func (c *cmdBase) isResponseTimeout() bool {
	return !c.responseDeadline.After(time.Now())
}
