package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskSet struct {
	cmd *cmdSet
}

func (t *sqlTaskSet) canCombine() bool {
	return false
}

func (t *sqlTaskSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskSet) do(db *sqlx.DB) {
	tableMeta := getDBMeta().getTableMeta(t.cmd.table)

	var (
		errCode int32
		version int64
		sqlStr  = getStr()
	)

	if t.cmd.version != nil {
		s := appendUpdateSqlStr(sqlStr, t.cmd.table, t.cmd.key, *t.cmd.version, t.cmd.fields).ToString()
		putStr(sqlStr)

		start := time.Now()
		result, err := db.Exec(s)
		getLogger().Debugf("task-set-with-version: table(%s) key(%s): query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err != nil {
			getLogger().Debugf("task-set-with-version: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n, err := result.RowsAffected(); err != nil {
			getLogger().Debugf("task-set-with-version: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			errCode = errcode.ERR_OK
			version = *t.cmd.version + 1
		} else {
			errCode = errcode.ERR_VERSION_MISMATCH
		}
	} else {
		sqlStr.AppendString("begin;")

		appendInsertOrUpdateSqlStr(sqlStr, tableMeta, t.cmd.key, 1, t.cmd.fields)

		sqlStr.AppendString("select ").AppendString(versionFieldName).AppendString(" from ").AppendString(t.cmd.table)
		sqlStr.AppendString(" where ").AppendString(keyFieldName).AppendString("=").AppendString("'").AppendString(t.cmd.key).AppendString("';")

		sqlStr.AppendString("end;")

		s := sqlStr.ToString()
		putStr(sqlStr)

		start := time.Now()
		row := db.QueryRowx(s)
		getLogger().Debugf("task-set: table(%s) key(%s): query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err := row.Scan(&version); err != nil {
			getLogger().Errorf("task-set: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else {
			errCode = errcode.ERR_OK
			version = 1
		}
	}

	t.cmd.reply(errCode, version, nil)
}

type cmdSet struct {
	cmdBase
	fields  map[string]*proto.Field
	version *int64
}

func (c *cmdSet) makeSqlTask() sqlTask {
	return &sqlTaskSet{cmd: c}
}

func (c *cmdSet) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.SetResp{Version: version}

		_ = c.conn.sendMessage(net.NewMessage(
			net.CommonHead{
				Seqno:   c.sqNo,
				ErrCode: errCode,
			},
			resp,
		))
	}
}

func onSet(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.SetReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)

	if tableMeta == nil {
		getLogger().Errorf("set table(%s) key(%s): table not exist.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if len(req.GetFields()) == 0 {
		getLogger().Errorf("set table(%s) key(%s): no fields.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_MISSING_FIELDS, &proto.GetResp{}))
		return
	}

	if b, i := tableMeta.checkFields(req.GetFields()); !b {
		getLogger().Errorf("set table(%s) key(%s): invalid field(%s).", table, key, req.GetFields()[i])
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdSet{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		fields:  make(map[string]*proto.Field, len(req.GetFields())),
		version: req.Version,
	}

	for _, v := range req.GetFields() {
		// check repeated field
		if cmd.fields[v.GetName()] != nil {
			getLogger().Errorf("set table(%s) key(%s): field(%s) repeated.", table, key, v.GetName())
			_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
			return
		}

		cmd.fields[v.GetName()] = v
	}

	pushCmd(cmd)
}
