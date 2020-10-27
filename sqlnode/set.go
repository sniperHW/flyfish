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

		getLogger().Debugf("task-set-with-version: table(%s) key(%s): start query: \"%s\".", t.cmd.table, t.cmd.key, s)

		if result, err := db.Exec(s); err != nil {
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

		appendInsertOrUpdateSqlStr(sqlStr, tableMeta, t.cmd.key, 0, t.cmd.fields)

		sqlStr.AppendString("select ").AppendString(versionFieldName).AppendString(" from ").AppendString(t.cmd.table)
		sqlStr.AppendString(" where ").AppendString(keyFieldName).AppendString("=").AppendString("'").AppendString(t.cmd.key).AppendString("';")

		sqlStr.AppendString("end;")

		s := sqlStr.ToString()
		putStr(sqlStr)

		getLogger().Debugf("task-set: table(%s) key(%s): start query: \"%s\".", t.cmd.table, t.cmd.key, s)

		start := time.Now()
		row := db.QueryRowx(s)
		getLogger().Debugf("task-set: table(%s) key(%s): query cost %.3f sec.", t.cmd.table, t.cmd.key, time.Now().Sub(start).Seconds())

		if err := row.Scan(&version); err != nil {
			getLogger().Errorf("task-set: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else {
			errCode = errcode.ERR_OK
		}
	}

	t.reply(errCode, version)
}

func (t *sqlTaskSet) reply(errCode int32, version int64) {
	t.cmd.reply(errCode, nil, version)
}

type cmdSet struct {
	cmdBase
	fields  map[string]*proto.Field
	version *int64
}

func (c *cmdSet) makeSqlTask() sqlTask {
	return &sqlTaskSet{cmd: c}
}

func (c *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
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
		getLogger().Errorf("get table(%s): table not exist.", table)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if len(req.GetFields()) == 0 {
		getLogger().Errorf("get table(%s): no fields.", table)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_MISSING_FIELDS, &proto.GetResp{}))
		return
	}

	if b, i := tableMeta.checkFields(req.GetFields()); !b {
		getLogger().Errorf("get table(%s): invalid field(%s).", table, req.GetFields()[i])
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdSet{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		fields:  make(map[string]*proto.Field, len(req.GetFields())),
		version: req.Version,
	}

	// todo check repeated field ?
	for _, v := range req.GetFields() {
		cmd.fields[v.GetName()] = v
	}

	pushCmd(cmd)
}
