package sqlnode

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
	"time"
)

type sqlTaskCompareSetNx struct {
	cmd *cmdCompareSetNx
}

func (t *sqlTaskCompareSetNx) canCombine() bool {
	return false
}

func (t *sqlTaskCompareSetNx) combine(cmd) bool {
	return false
}

func (t *sqlTaskCompareSetNx) do(db *sqlx.DB) {
	var (
		tableMeta = getDBMeta().getTableMeta(t.cmd.table)
		errCode   int32
		version   int64
		table     = t.cmd.table
		key       = t.cmd.key
		valueName = t.cmd.old.GetName()
		oldValue  = t.cmd.old
		newValue  = t.cmd.new
		retValue  *proto.Field
		tx        *sqlx.Tx
		err       error
		sqlStr    *str.Str
	)

	if tx, err = db.Beginx(); err != nil {
		getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): begin transaction: %s.", table, key, err)
		errCode = errcode.ERR_SQLERROR
	} else {
		var (
			curVersion    int64
			fieldMeta     = tableMeta.getFieldMeta(valueName)
			valueReceiver = fieldMeta.getReceiver()
			success       = false
			s             string
			row           *sqlx.Row
			result        sql.Result
			n             int64
		)

		sqlStr = getStr()

		sqlStr.AppendString("SELECT ").AppendString(versionFieldName).AppendString(",").AppendString(valueName).AppendString(" ")
		sqlStr.AppendString("FROM ").AppendString(table).AppendString(" ")
		sqlStr.AppendString("WHERE ").AppendString(keyFieldName).AppendString("='").AppendString(key).AppendString("';")

		s = sqlStr.ToString()
		start := time.Now()
		row = tx.QueryRowx(s)
		getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): select query:\"%s\" cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

		if err = row.Scan(&curVersion, valueReceiver); err != nil {
			if err == sql.ErrNoRows {
				// 记录不存在，插入新记录

				sqlStr.Reset()
				appendInsertSqlStr(sqlStr, tableMeta, key, 1, map[string]*proto.Field{valueName: newValue})

				s = sqlStr.ToString()
				start = time.Now()
				result, err = tx.Exec(s)
				getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): insert query:\"%s\" cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

				if err == nil {
					n, err = result.RowsAffected()
				}

				if err != nil {
					getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert: %s.", table, key, err)
					errCode = errcode.ERR_SQLERROR
				} else if n > 0 {
					// 插入成功

					errCode = errcode.ERR_OK
					version = 1
					retValue = newValue
					success = true

				} else {
					// 插入失败, impossible

					getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert failed - impossible.", table, key)
					errCode = errcode.ERR_OTHER

				}

			} else {
				getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): select: %s.", table, key, err)
				errCode = errcode.ERR_SQLERROR
			}
		} else {
			// 记录存在

			var (
				curValue = proto.PackField(valueName, fieldMeta.getConverter()(valueReceiver))
			)

			if t.cmd.version != nil && *t.cmd.version != curVersion {
				// 版本不匹配

				errCode = errcode.ERR_VERSION_MISMATCH
				version = curVersion
				retValue = curValue

			} else if !curValue.IsEqual(oldValue) {
				// 值不匹配

				errCode = errcode.ERR_CAS_NOT_EQUAL
				version = curVersion
				retValue = curValue

			} else {
				// 更新 value

				var (
					newVersion = curVersion + 1
					result     sql.Result
					n          int64
				)

				sqlStr.Reset()
				appendUpdateSqlStr(sqlStr, table, key, &curVersion, &newVersion, []*proto.Field{newValue})

				s = sqlStr.ToString()
				start = time.Now()
				result, err = tx.Exec(s)
				getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): update query: %s cost:%.3f.", table, key, s, time.Now().Sub(start).Seconds())

				if err == nil {
					n, err = result.RowsAffected()
				}

				if err != nil {
					getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): update: %s.", table, key, err)
					errCode = errcode.ERR_SQLERROR
				} else if n > 0 {
					// 更新成功

					errCode = errcode.ERR_OK
					version = newVersion
					retValue = newValue
					success = true

				} else {
					// 更新失败，理论上 impossible

					getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): update failed - impossible.", table, key)
					errCode = errcode.ERR_OTHER

				}

			}

		}

		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): transaction-commit: %s.", t.cmd.table, t.cmd.key, err)
			} else {
				getLogger().Infof("task-compare-set-nx: table(%s) key(%s): transaction-commit.", t.cmd.table, t.cmd.key)
			}
		} else if err = tx.Rollback(); err != nil {
			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): transaction-rollback: %s.", t.cmd.table, t.cmd.key, err)
		} else {
			getLogger().Infof("task-compare-set-nx: table(%s) key(%s): transaction-rollback.", t.cmd.table, t.cmd.key)
		}

		if err != nil {
			errCode = errcode.ERR_SQLERROR
			version = 0
			retValue = nil
		}

		putStr(sqlStr)
	}

	t.cmd.reply(errCode, version, retValue)
}

type cmdCompareSetNx struct {
	cmdBase
	version *int64
	old     *proto.Field
	new     *proto.Field
}

func (c *cmdCompareSetNx) canCombine() bool {
	return false
}

func (c *cmdCompareSetNx) makeSqlTask() sqlTask {
	return &sqlTaskCompareSetNx{cmd: c}
}

func (c *cmdCompareSetNx) reply(errCode int32, version int64, value *proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.CompareAndSetNxResp{
			Version: version,
			Value:   value,
		}

		_ = c.conn.sendMessage(net.NewMessage(
			net.CommonHead{
				Seqno:   c.sqNo,
				ErrCode: errCode,
			},
			resp,
		))
	}
}

func onCompareSetNx(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)

	if req.Old == nil || req.New == nil || req.Old.GetName() != req.New.GetName() || req.Old.GetType() != req.New.GetType() {
		getLogger().Errorf("compare-set-nx table(%s) key(%s): value type not match.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.CompareAndSetResp{}))
		return
	}

	if req.Old.IsEqual(req.New) {
		getLogger().Errorf("compare-set-nx table(%s) key(%s): value not change.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_RECORD_UNCHANGE, &proto.CompareAndSetResp{}))
		return
	}

	if tableMeta == nil {
		getLogger().Errorf("compare-set-nx table(%s) key(%s): table not exist.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if !tableMeta.checkField(req.Old) {
		getLogger().Errorf("compare-set-nx table(%s) key(%s): invalid field(%s).", table, key, req.Old.GetName())
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdCompareSetNx{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		version: req.Version,
		old:     req.Old,
		new:     req.New,
	}

	pushCmd(cmd)
}
