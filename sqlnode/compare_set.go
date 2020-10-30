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

func (t *sqlTaskCompareSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskCompareSet) do(db *sqlx.DB) {
	tableMeta := getDBMeta().getTableMeta(t.cmd.table)

	var (
		errCode    int32
		version    int64
		valueField *proto.Field
		tx         *sqlx.Tx
		err        error
		sqlStr     = getStr()
	)

	if tx, err = db.Beginx(); err != nil {
		getLogger().Errorf("task-compare-set: table(%s) key(%s): begin-transaction: %s.", t.cmd.table, t.cmd.key, err)
		errCode = errcode.ERR_SQLERROR
	} else {
		var (
			s      string
			result sql.Result
			n      int64
		)

		getLogger().Infof("task-compare-set: table(%s) key(%s): begin-transaction.", t.cmd.table, t.cmd.key)

		sqlStr.AppendString("update ").AppendString(t.cmd.table).AppendString(" set ")
		sqlStr.AppendString(versionFieldName).AppendString("=").AppendString(versionFieldName).AppendString("+1,")
		sqlStr.AppendString(t.cmd.new.GetName()).AppendString("=")
		appendFieldValue2SqlStr(sqlStr, t.cmd.new).AppendString(" where ").AppendString(keyFieldName).AppendString("='").AppendString(t.cmd.key).AppendString("'")
		if t.cmd.version != nil {
			sqlStr.AppendString(" and ").AppendString(versionFieldName).AppendString("=")
			appendValue2SqlStr(sqlStr, versionFieldMeta.getType(), *t.cmd.version)
		}
		sqlStr.AppendString(" and ").AppendString(t.cmd.old.GetName()).AppendString("=")
		appendFieldValue2SqlStr(sqlStr, t.cmd.old).AppendString(";")

		s = sqlStr.ToString()

		// 尝试更新数据
		start := time.Now()
		result, err = tx.Exec(s)
		getLogger().Debugf("task-compare-set: table(%s) key(%s): update query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		commit := false

		if err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): update: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n, err = result.RowsAffected(); err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): update: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			// 更新成功

			if t.cmd.version != nil {
				// 直接计算出新的 version

				errCode = errcode.ERR_OK
				version = *t.cmd.version + 1
				valueField = t.cmd.new
				commit = true
			} else {
				// 查询 version

				sqlStr.Reset()
				sqlStr.AppendString("select ").AppendString(versionFieldName).AppendString(" from ").AppendString(t.cmd.table).AppendString(" where ")
				sqlStr.AppendString(keyFieldName).AppendString("='").AppendString(t.cmd.key).AppendString("';")

				s = sqlStr.ToString()

				start := time.Now()
				row := tx.QueryRowx(s)
				getLogger().Debugf("task-compare-set: table(%s) key(%s): select version query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

				if err = row.Scan(&version); err != nil {
					if err == sql.ErrNoRows {
						errCode = errcode.ERR_RECORD_NOTEXIST
					} else {
						getLogger().Errorf("task-compare-set: table(%s) key(%s): select version: %s.", t.cmd.table, t.cmd.key, err)
						errCode = errcode.ERR_SQLERROR
					}
				} else {
					commit = true
				}
			}

		} else {
			// 先回滚结束事务，再查询 version 和 字段值

			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-rollback: %s.", t.cmd.table, t.cmd.key, err)
				errCode = errcode.ERR_SQLERROR
			} else {
				getLogger().Infof("task-compare-set: table(%s) key(%s): transaction-rollback.", t.cmd.table, t.cmd.key)

				fieldMeta := tableMeta.getFieldMeta(t.cmd.old.GetName())
				valueReceiver := fieldMeta.getReceiver()

				sqlStr.Reset()
				sqlStr.AppendString("select ").AppendString(versionFieldName).AppendString(",").AppendString(t.cmd.new.GetName()).AppendString(" from ").AppendString(t.cmd.table).AppendString(" where ")
				sqlStr.AppendString(keyFieldName).AppendString("='").AppendString(t.cmd.key).AppendString("';")

				s = sqlStr.ToString()

				start := time.Now()
				row := db.QueryRowx(s)
				getLogger().Debugf("task-compare-set: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

				if err = row.Scan(&version, valueReceiver); err != nil {
					if err == sql.ErrNoRows {
						errCode = errcode.ERR_RECORD_NOTEXIST
					} else {
						getLogger().Errorf("task-compare-set: table(%s) key(%s): select: %s.", t.cmd.table, t.cmd.key, err)
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

			goto reply
		}

		if commit {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-commit: %s.", t.cmd.table, t.cmd.key, err)
			} else {
				getLogger().Infof("task-compare-set: table(%s) key(%s): transaction-commit.", t.cmd.table, t.cmd.key)
			}
		} else if err = tx.Rollback(); err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-rollback: %s.", t.cmd.table, t.cmd.key, err)
		} else {
			getLogger().Infof("task-compare-set: table(%s) key(%s): transaction-rollback.", t.cmd.table, t.cmd.key)
		}

		if err != nil {
			errCode = errcode.ERR_SQLERROR
			version = 0
			valueField = nil
		}
	}

reply:
	putStr(sqlStr)
	t.cmd.reply_(errCode, version, valueField)
}

type cmdCompareSet struct {
	cmdBase
	version *int64
	old     *proto.Field
	new     *proto.Field
}

func (c *cmdCompareSet) makeSqlTask() sqlTask {
	return &sqlTaskCompareSet{cmd: c}
}

func (c *cmdCompareSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	//panic("implement me")
}

func (c *cmdCompareSet) reply_(errCode int32, version int64, value *proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.CompareAndSetResp{
			Version: version,
			Value:   value,
		}

		_ = c.conn.sendMessage(net.NewMessage(
			net.CommonHead{
				Seqno:   c.sqNo,
				UniKey:  "",
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
