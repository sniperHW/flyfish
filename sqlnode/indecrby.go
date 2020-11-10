package sqlnode

import (
	"database/sql"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
	"time"
)

type sqlTaskInDeCrBy struct {
	cmd *cmdInDeCrBy
}

func (t *sqlTaskInDeCrBy) canCombine() bool {
	return false
}

func (t *sqlTaskInDeCrBy) combine(cmd) bool {
	return false
}

func (t *sqlTaskInDeCrBy) do(db *sqlx.DB) {
	var (
		table   = t.cmd.table
		key     = t.cmd.key
		field   = t.cmd.delta
		errCode int32
		version int64
		tx      *sqlx.Tx
		err     error
	)

	tx, err = db.Beginx()
	if err != nil {
		getLogger().Errorf("task-indecrby: table(%s) key(%s): begin-transaction: %s.", table, key, err)
		errCode = errcode.ERR_SQLERROR
	} else {
		var (
			sqlStr         *str.Str
			tableMeta      = getDBMeta().getTableMeta(t.cmd.table)
			fieldName      = t.cmd.delta.GetName()
			success        bool
			fieldMeta      = tableMeta.getFieldMeta(fieldName)
			fieldReceiver  = fieldMeta.getReceiver()
			s              string
			row            *sqlx.Row
			recordNotExist bool
		)

		sqlStr = getStr()

		sqlStr.AppendString("SELECT ").AppendString(versionFieldName).AppendString(",").AppendString(fieldName)
		sqlStr.AppendString(" FROM ").AppendString(table)
		sqlStr.AppendString(" WHERE ").AppendString(keyFieldName).AppendString("='").AppendString(key).AppendString("'").AppendString(";")

		s = sqlStr.ToString()
		start := time.Now()
		fmt.Println(s)
		row = tx.QueryRowx(s)
		getLogger().Debugf("task-indecrby: table(%s) key(%s): select query:\"%s\" cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

		if err = row.Scan(&version, fieldReceiver); err != nil {
			if err == sql.ErrNoRows {
				err = nil
				recordNotExist = true
			} else {
				getLogger().Errorf("task-indecrby: table(%s) key(%s): select: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
			}
		}

		if err == nil {
			var (
				valueDelta = t.cmd.delta
				fields     = map[string]*proto.Field{fieldName: field}
				result     sql.Result
			)

			sqlStr.Reset()

			if recordNotExist {
				// 插入

				if t.cmd.incr {
					field.SetInt(fieldMeta.getDefaultV().(int64) + valueDelta.GetInt())
				} else {
					field.SetInt(fieldMeta.getDefaultV().(int64) - valueDelta.GetInt())
				}

				appendInsertSqlStr(sqlStr, tableMeta, key, 1, fields)

				s = sqlStr.ToString()
				start = time.Now()
				result, err = tx.Exec(s)
				getLogger().Debugf("task-indecrby: table(%s) key(%s): insert query:\"%s\" cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

				if err != nil {
					getLogger().Errorf("task-indecrby: table(%s) key(%s): insert: %s.", table, key, s)
					errCode = errcode.ERR_SQLERROR
				} else if _, err = result.RowsAffected(); err != nil {
					getLogger().Errorf("task-indecrby: table(%s) key(%s): insert: %s.", table, key, s)
					errCode = errcode.ERR_SQLERROR
				} else {
					errCode = errcode.ERR_OK
					version = 1
					success = true
				}

			} else {
				// 更新

				if t.cmd.incr {
					field.SetInt(fieldMeta.getConverter()(fieldReceiver).(int64) + valueDelta.GetInt())
				} else {
					field.SetInt(fieldMeta.getConverter()(fieldReceiver).(int64) - field.GetInt())
				}

				if t.cmd.version != nil && *t.cmd.version != version {
					errCode = errcode.ERR_VERSION_MISMATCH
				} else {
					appendUpdateSqlStr(sqlStr, table, key, version, fields)

					s = sqlStr.ToString()
					start = time.Now()
					result, err = tx.Exec(s)
					getLogger().Debugf("task-indecrby: table(%s) key(%s): update query:\"%s\" cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

					if err != nil {
						getLogger().Errorf("task-indecrby: table(%s) key(%s): update: %s.", table, key, s)
						errCode = errcode.ERR_SQLERROR
					} else if _, err = result.RowsAffected(); err != nil {
						getLogger().Errorf("task-indecrby: table(%s) key(%s): update: %s.", table, key, s)
						errCode = errcode.ERR_SQLERROR
					} else {
						errCode = errcode.ERR_OK
						version = version + 1
						success = true
					}
				}

			}
		}

		putStr(sqlStr)

		if success {

			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-indecrby: table(%s) key(%s): commit-transaction: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
				version = 0
				field = nil
			}

		} else {

			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-indecrby: table(%s) key(%s): rollback-transaction: %s.", table, key, err)
			}

		}
	}

	t.cmd.reply_(errCode, field, version)
}

type cmdInDeCrBy struct {
	cmdBase
	delta   *proto.Field
	version *int64
	incr    bool
}

func (c *cmdInDeCrBy) makeSqlTask() sqlTask {
	return &sqlTaskInDeCrBy{cmd: c}
}

func (c *cmdInDeCrBy) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		var resp pb.Message
		var field *proto.Field

		if len(fields) > 0 {
			field = fields[c.delta.GetName()]
		}

		if c.incr {
			resp = &proto.IncrByResp{
				Version: version,
				Field:   field,
			}
		} else {
			resp = &proto.DecrByResp{
				Version: version,
				Field:   field,
			}
		}

		_ = c.conn.sendMessage(newMessage(c.sqNo, errCode, resp))
	}
}

func (c *cmdInDeCrBy) reply_(errCode int32, field *proto.Field, version int64) {

}

func onIncrBy(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.IncrByReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)
	if tableMeta == nil {
		getLogger().Errorf("incrby table(%s) key(%s): invalid table.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, new(proto.IncrByResp)))
		return
	}

	if req.Field == nil || req.Field.GetType() != proto.ValueType_int || !tableMeta.checkField(req.Field) {
		getLogger().Errorf("incrby table(%s) key(%s): invalid field(%s).", table, key, req.Field.GetName())
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, new(proto.IncrByResp)))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdInDeCrBy{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		delta:   req.Field,
		version: req.Version,
		incr:    true,
	}

	pushCmd(cmd)
}

func onDecrBy(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.DecrByReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)
	if tableMeta == nil {
		getLogger().Errorf("decrby table(%s) key(%s): invalid table.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, new(proto.IncrByResp)))
		return
	}

	if req.Field == nil || req.Field.GetType() != proto.ValueType_int || !tableMeta.checkField(req.Field) {
		getLogger().Errorf("decrby table(%s) key(%s): invalid field(%s).", table, key, req.Field.GetName())
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, new(proto.IncrByResp)))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdInDeCrBy{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		delta:   req.Field,
		version: req.Version,
		incr:    false,
	}

	pushCmd(cmd)
}
