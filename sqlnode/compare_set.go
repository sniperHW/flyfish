package sqlnode

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskCompareSet struct {
	cmd *cmdCompareSet
}

func (t *sqlTaskCompareSet) canCombine() bool {
	return false
}

func (t *sqlTaskCompareSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskCompareSet) do(db *sqlx.DB) {
	var (
		table      = t.cmd.table
		key        = t.cmd.key
		tableMeta  = getDBMeta().getTableMeta(t.cmd.table)
		errCode    int32
		version    int64
		valueField *proto.Field
		tx         *sqlx.Tx
		err        error
	)

	if tx, err = db.Beginx(); err != nil {
		getLogger().Errorf("task-compare-set: table(%s) key(%s): begin-transaction: %s.", table, key, err)
		errCode = errcode.ERR_SQLERROR
	} else {
		var (
			valueName = t.cmd.old.GetName()
			sqlStr    = getStr()
			s         string
			result    sql.Result
			n         int64
		)

		appendSingleUpdateSqlStr(sqlStr, table, key, t.cmd.version, nil, []*proto.Field{t.cmd.new}, sqlCond{sqlCondEqual, t.cmd.old})

		s = sqlStr.ToString()
		start := time.Now()
		result, err = tx.Exec(s)
		getLogger().Debugf("task-compare-set: table(%s) key(%s): update query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

		success := false

		if err == nil {
			n, err = result.RowsAffected()
		}

		if err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): update: %s.", table, key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			// 更新成功

			if t.cmd.version != nil {
				// 直接计算出新的 version

				errCode = errcode.ERR_OK
				version = *t.cmd.version + 1
				valueField = t.cmd.new
				success = true
			} else {
				// 查询 version

				sqlStr.Reset()
				appendSingleSelectFieldsSqlStr(sqlStr, table, key, nil, []string{versionFieldName})

				s = sqlStr.ToString()
				start := time.Now()
				row := tx.QueryRowx(s)
				getLogger().Debugf("task-compare-set: table(%s) key(%s): select version query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

				if err = row.Scan(&version); err != nil {
					if err == sql.ErrNoRows {
						errCode = errcode.ERR_RECORD_NOTEXIST
					} else {
						getLogger().Errorf("task-compare-set: table(%s) key(%s): select version: %s.", table, key, err)
						errCode = errcode.ERR_SQLERROR
					}
				} else {
					errCode = errcode.ERR_OK
					valueField = t.cmd.new
					success = true
				}
			}

		} else {
			var (
				fieldMeta     = tableMeta.getFieldMeta(valueName)
				valueReceiver = fieldMeta.getReceiver()
			)

			sqlStr.Reset()
			appendSingleSelectFieldsSqlStr(sqlStr, table, key, nil, []string{versionFieldName, valueName})

			s = sqlStr.ToString()
			start := time.Now()
			row := db.QueryRowx(s)
			getLogger().Debugf("task-compare-set: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", table, key, s, time.Now().Sub(start).Seconds())

			if err = row.Scan(&version, valueReceiver); err != nil {
				if err == sql.ErrNoRows {
					errCode = errcode.ERR_RECORD_NOTEXIST
				} else {
					getLogger().Errorf("task-compare-set: table(%s) key(%s): select: %s.", table, key, err)
					errCode = errcode.ERR_SQLERROR
				}
			} else {
				valueField = proto.PackField(t.cmd.new.GetName(), fieldMeta.getConverter()(valueReceiver))

				if t.cmd.version != nil && version != *t.cmd.version {
					errCode = errcode.ERR_VERSION_MISMATCH
				} else {
					errCode = errcode.ERR_CAS_NOT_EQUAL
				}
			}
		}

		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-commit: %s.", table, key, err)
			}
		} else {
			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-rollback: %s.", table, key, err)
			}
		}

		if err != nil {
			errCode = errcode.ERR_SQLERROR
			version = 0
			valueField = nil
		}

		putStr(sqlStr)
	}

	t.cmd.reply(errCode, version, valueField)
}

type cmdCompareSet struct {
	cmdBase
	version *int64
	old     *proto.Field
	new     *proto.Field
}

func (c *cmdCompareSet) canCombine() bool {
	return false
}

func (c *cmdCompareSet) makeSqlTask() sqlTask {
	return &sqlTaskCompareSet{cmd: c}
}

func (c *cmdCompareSet) reply(errCode int32, version int64, value *proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.CompareAndSetResp{
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

func onCompareSet(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.CompareAndSetReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)

	if req.Old == nil || req.New == nil || req.Old.GetName() != req.New.GetName() || req.Old.GetType() != req.New.GetType() {
		getLogger().Errorf("compare-set table(%s) key(%s): value type not match.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.CompareAndSetResp{}))
		return
	}

	if req.Old.IsEqual(req.New) {
		getLogger().Errorf("compare-set table(%s) key(%s): value not change.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_RECORD_UNCHANGE, &proto.CompareAndSetResp{}))
		return
	}

	if tableMeta == nil {
		getLogger().Errorf("compare-set table(%s) key(%s): table not exist.", table, key)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if !tableMeta.checkField(req.Old) {
		getLogger().Errorf("compare-set table(%s) key(%s): invalid field(%s).", table, key, req.Old.GetName())
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdCompareSet{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		version: req.Version,
		old:     req.Old,
		new:     req.New,
	}

	pushCmd(cmd)
}
