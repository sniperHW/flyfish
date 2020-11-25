package sqlnode

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskDel struct {
	sqlTaskBase
	version *int64
}

func (t *sqlTaskDel) canCombine() bool {
	return false
}

func (t *sqlTaskDel) combine(cmd) bool {
	return false
}

func (t *sqlTaskDel) do(db *sqlx.DB) (errCode int32, version int64, fields map[string]*proto.Field) {
	var (
		sqlStr = getStr()
		result sql.Result
		n      int64
		err    error
	)

	defer putStr(sqlStr)

	appendSingleDeleteSqlStr(sqlStr, t.table, t.key, t.version)

	s := sqlStr.ToString()
	start := time.Now()
	result, err = db.Exec(s)
	getLogger().Debugf("task-del: table(%s) key(%s): delete query:\"%s\" cost:%.3fs.", t.table, t.key, s, time.Now().Sub(start).Seconds())

	if err != nil {
		getLogger().Errorf("task-del: table(%s) key(%s): delete: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	if n, err = result.RowsAffected(); err != nil {
		getLogger().Errorf("task-del: table(%s) key(%s): delete: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	if n <= 0 {
		if t.version != nil {
			errCode = errcode.ERR_VERSION_MISMATCH
		} else {
			errCode = errcode.ERR_RECORD_NOTEXIST
		}
		return
	}

	errCode = errcode.ERR_OK

	return
}

type cmdDel struct {
	cmdBase
	version *int64
}

func (c *cmdDel) canCombine() bool {
	return false
}

func (c *cmdDel) makeSqlTask() sqlTask {
	return &sqlTaskDel{sqlTaskBase: newSqlTaskBase(c.table, c.key), version: c.version}
}

func (c *cmdDel) replyError(errCode int32) {
	c.reply(errCode, 0, nil)
}

func (c *cmdDel) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		_ = c.conn.sendMessage(newMessage(c.sqNo, errCode, new(proto.DelResp)))
	}
}

func onDel(cli *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.DelReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)
	if tableMeta == nil {
		getLogger().Errorf("del table(%s) key(%s): invalid table.", table, key)
		_ = cli.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, new(proto.DelResp)))
		return
	}

	procDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdDel{
		cmdBase: newCmdBase(cli, head.Seqno, head.UniKey, table, key, procDeadline, respDeadline),
		version: req.Version,
	}

	processCmd(cmd)
}
