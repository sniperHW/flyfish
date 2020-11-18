package sqlnode

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskSetNx struct {
	cmd *cmdSetNx
}

func (t *sqlTaskSetNx) canCombine() bool {
	return false
}

func (t *sqlTaskSetNx) combine(cmd) bool {
	return false
}

func (t *sqlTaskSetNx) do(db *sqlx.DB) {
	var (
		table     = t.cmd.table
		key       = t.cmd.key
		tableMeta = getDBMeta().getTableMeta(table)
		errCode   int32
		version   int64
		fields    []*proto.Field
		tx        *sqlx.Tx
		err       error
	)

	tx, err = db.Beginx()
	if err != nil {
		getLogger().Errorf("task-set-nx: table(%s) key(%s): begin-transaction: %s.", err)
		errCode = errcode.ERR_SQLERROR
	} else {
		var (
			sqlStr               = getStr()
			getFieldCount        int
			queryFieldCount      int
			queryFields          []string
			queryFieldReceivers  []interface{}
			queryFieldConverters []fieldConverter
			i                    int
			s                    string
			start                time.Time
			row                  *sqlx.Row
			success              bool
		)

		getFieldCount = len(t.cmd.fields)
		queryFieldCount = getFieldCount + 1
		queryFields = make([]string, queryFieldCount)
		queryFieldReceivers = make([]interface{}, queryFieldCount)
		queryFieldConverters = make([]fieldConverter, queryFieldCount)
		queryFields[0] = versionFieldName
		queryFieldReceivers[0] = versionFieldMeta.getReceiver()
		queryFieldConverters[0] = versionFieldMeta.getConverter()
		i = 1
		for k, _ := range t.cmd.fields {
			fm := tableMeta.getFieldMeta(k)
			queryFields[i] = k
			queryFieldReceivers[i] = fm.getReceiver()
			queryFieldConverters[i] = fm.getConverter()
			i++
		}

		appendSingleSelectFieldsSqlStr(sqlStr, table, key, nil, queryFields)

		s = sqlStr.ToString()
		start = time.Now()
		row = tx.QueryRowx(s)
		getLogger().Debugf("task-set-nx: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

		err = row.Scan(queryFieldReceivers...)

		if err == nil {
			// 记录存在

			errCode = errcode.ERR_RECORD_EXIST
			version = queryFieldConverters[0](queryFieldReceivers[0]).(int64)
			fields = make([]*proto.Field, 0, getFieldCount)

			for i = 1; i < queryFieldCount; i++ {
				fieldName := queryFields[i]
				fields = append(fields, proto.PackField(fieldName, queryFieldConverters[i](queryFieldReceivers[i])))
			}

			success = true

		} else if err == sql.ErrNoRows {
			// 记录不存在

			var (
				result sql.Result
				n      int64
			)

			sqlStr.Reset()
			appendInsertSqlStr(sqlStr, tableMeta, key, 1, t.cmd.fields)
			s = sqlStr.ToString()
			start = time.Now()
			result, err = tx.Exec(s)
			getLogger().Debugf("task-set-nx: table(%s) key(%s): insert query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

			if err == nil {
				n, err = result.RowsAffected()
			}

			if err != nil {
				getLogger().Errorf("task-set-nx: table(%s) key(%s): insert: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
			} else if n > 0 {
				errCode = errcode.ERR_OK
				version = 1
				success = true
			} else {
				getLogger().Errorf("task-set-nx: table(%s) key(%s): record exist - impossible.", table, key)
				errCode = errcode.ERR_RECORD_EXIST
			}

		} else {
			getLogger().Errorf("task-set-nx: table(%s) key(%s): select: %s.", table, key, err)
			errCode = errcode.ERR_SQLERROR
		}

		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-set-nx: table(%s) key(%s): transaction-commit: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
				version = 0
				fields = nil
			}
		} else {
			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-set-nx: table(%s) key(%s): transaction-rollback: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
			}
		}

		putStr(sqlStr)
	}

	t.cmd.reply(errCode, version, fields)
}

type cmdSetNx struct {
	cmdBase
	fields  map[string]*proto.Field
	version *int64
}

func (c *cmdSetNx) canCombine() bool {
	return false
}

func (c *cmdSetNx) makeSqlTask() sqlTask {
	return &sqlTaskSetNx{cmd: c}
}

func (c *cmdSetNx) reply(errCode int32, version int64, fields []*proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.SetNxResp{}

		if errCode == errcode.ERR_OK {
			resp.Version = version
		} else if errCode == errcode.ERR_RECORD_EXIST {
			resp.Version = version
			resp.Fields = fields
			//resp.Fields = make([]*proto.Field, len(c.fields))
			//tableMeta := getDBMeta().getTableMeta(c.table)
			//i := 0
			//for k, _ := range c.fields {
			//	f := fields[k]
			//	if f != nil {
			//		resp.Fields[i] = f
			//	} else {
			//		// todo impossible in current design.
			//		resp.Fields[i] = proto.PackField(k, tableMeta.getFieldMeta(k).getDefaultV())
			//	}
			//	i++
			//}
		}

		_ = c.conn.sendMessage(
			net.NewMessage(net.CommonHead{
				Seqno:   c.sqNo,
				ErrCode: errCode,
			},
				resp,
			),
		)
	}
}

func onSetNx(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.SetNxReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)

	if tableMeta == nil {
		getLogger().Errorf("set-nx table(%s) key(%s): table not exist.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if len(req.GetFields()) == 0 {
		getLogger().Errorf("set-nx table(%s) key(%s): no fields.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_MISSING_FIELDS, &proto.GetResp{}))
		return
	}

	if b, i := tableMeta.checkFields(req.GetFields()); !b {
		getLogger().Errorf("set-nx table(%s) key(%s): invalid field(%s).", table, key, req.GetFields()[i])
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdSetNx{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		fields:  make(map[string]*proto.Field, len(req.GetFields())),
		version: req.Version,
	}

	for _, v := range req.GetFields() {
		// check repeated field
		if cmd.fields[v.GetName()] != nil {
			getLogger().Errorf("set-nx table(%s) key(%s): field(%s) repeated.", table, key, v.GetName())
			_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
			return
		}

		cmd.fields[v.GetName()] = v
	}

	pushCmd(cmd)
}
