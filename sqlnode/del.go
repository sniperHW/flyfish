package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskDel struct {
	cmd *cmdDel
}

func (t *sqlTaskDel) combine(cmd) bool {
	return false
}

func (t *sqlTaskDel) do(db *sqlx.DB) {
	var (
		table   = t.cmd.table
		key     = t.cmd.key
		errCode int32
		version int64
		sqlStr  = getStr()
	)

	sqlStr.AppendString("delete from ").AppendString(table)
	sqlStr.AppendString(" where ").AppendString(keyFieldName).AppendString("='").AppendString(key).AppendString("'")
	if t.cmd.version != nil {
		sqlStr.AppendString(" and ").AppendString(versionFieldName).AppendString("=")
		appendValue2SqlStr(sqlStr, versionFieldMeta.getType(), *t.cmd.version)
	}
	sqlStr.AppendString(";")

	s := sqlStr.ToString()
	start := time.Now()
	result, err := db.Exec(s)
	getLogger().Debugf("task-del: table(%s) key(%s): delete query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

	if err != nil {
		getLogger().Errorf("task-del: table(%s) key(%s): delete: %s.", table, key, err)
		errCode = errcode.ERR_SQLERROR
	} else if n, err := result.RowsAffected(); err != nil {
		getLogger().Errorf("task-del: table(%s) key(%s): delete: %s.", table, key, err)
		errCode = errcode.ERR_SQLERROR
	} else if n > 0 {
		errCode = errcode.ERR_OK
	} else {
		if t.cmd.version != nil {
			errCode = errcode.ERR_VERSION_MISMATCH
		} else {
			errCode = errcode.ERR_RECORD_NOTEXIST
		}
	}

	t.cmd.reply(errCode, version, nil)
}

type cmdDel struct {
	cmdBase
	version *int64
}

func (c *cmdDel) makeSqlTask() sqlTask {
	return &sqlTaskDel{cmd: c}
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

	pushCmd(cmd)
}
