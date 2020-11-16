package sqlnode

import (
	"database/sql"
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
		err     error
		sqlStr  = getStr()
	)

	if t.cmd.version != nil {
		var (
			newVersion = *t.cmd.version + 1
			result     sql.Result
			n          int64
		)

		s := appendUpdateSqlStr(sqlStr, t.cmd.table, t.cmd.key, t.cmd.version, &newVersion, t.cmd.fields).ToString()
		start := time.Now()
		result, err = db.Exec(s)
		getLogger().Debugf("task-set-with-version: table(%s) key(%s): query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err == nil {
			n, err = result.RowsAffected()
		}

		if err != nil {
			getLogger().Debugf("task-set-with-version: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			errCode = errcode.ERR_OK
			version = *t.cmd.version + 1
		} else {
			errCode = errcode.ERR_VERSION_MISMATCH
		}
	} else {
		sqlStr.AppendString("BEGIN;")

		appendInsertOrUpdateSqlStr(sqlStr, tableMeta, t.cmd.key, 1, t.cmd.fieldMap)

		sqlStr.AppendString("SELECT ").AppendString(versionFieldName).AppendString(" FROM ").AppendString(t.cmd.table).AppendString(" ")
		sqlStr.AppendString("WHERE ").AppendString(keyFieldName).AppendString("=").AppendString("'").AppendString(t.cmd.key).AppendString("';")

		sqlStr.AppendString("END;")

		s := sqlStr.ToString()
		start := time.Now()
		row := db.QueryRowx(s)
		getLogger().Debugf("task-set: table(%s) key(%s): query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err := row.Scan(&version); err != nil {
			getLogger().Errorf("task-set: table(%s) key(%s): %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else {
			errCode = errcode.ERR_OK
			//version = 1
		}
	}

	putStr(sqlStr)
	t.cmd.reply(errCode, version)
}

type cmdSet struct {
	cmdBase
	fields   []*proto.Field
	fieldMap map[string]*proto.Field
	version  *int64
}

func (c *cmdSet) canCombine() bool {
	return false
}

func (c *cmdSet) makeSqlTask() sqlTask {
	return &sqlTaskSet{cmd: c}
}

func (c *cmdSet) reply(errCode int32, version int64) {
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
	}

	if req.Version != nil {
		cmd.fields = req.GetFields()
		cmd.version = req.Version
	} else {
		fieldMap := make(map[string]*proto.Field, len(req.GetFields()))
		for _, v := range req.GetFields() {
			// check repeated field
			if fieldMap[v.GetName()] != nil {
				getLogger().Errorf("set table(%s) key(%s): field(%s) repeated.", table, key, v.GetName())
				_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
				return
			}

			fieldMap[v.GetName()] = v
		}
		cmd.fieldMap = fieldMap
	}

	pushCmd(cmd)
}
