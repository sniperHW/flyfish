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
		execer     sqlx.Execer  = db
		queryer    sqlx.Queryer = db
		tableMeta               = getDBMeta().getTableMeta(t.cmd.table)
		errCode    int32
		version    int64
		valueField *proto.Field
		tx         *sqlx.Tx
		err        error
		sqlStr     *str.Str
	)

	if t.cmd.version == nil {
		if tx, err = db.Beginx(); err != nil {
			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): begin transaction: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else {
			getLogger().Infof("task-compare-set-nx: table(%s) key(%s): begin transaction.", t.cmd.table, t.cmd.key)
			execer = tx
			queryer = tx
		}
	}

	if err == nil {
		fields := map[string]*proto.Field{
			t.cmd.new.GetName(): t.cmd.new,
		}

		sqlStr = getStr()

		sqlStr.AppendString(tableMeta.getInsertPrefix()).AppendString("'").AppendString(t.cmd.key).AppendString("'").AppendString(",")
		appendValue2SqlStr(sqlStr, versionFieldMeta.getType(), int64(1))

		for _, v := range tableMeta.getFieldInsertOrder() {
			sqlStr.AppendString(",")

			f := fields[v]
			if f != nil {
				appendFieldValue2SqlStr(sqlStr, f)
			} else {
				fm := tableMeta.getFieldMeta(v)
				appendValue2SqlStr(sqlStr, fm.getType(), fm.getDefaultV())
			}
		}

		sqlStr.AppendString(") ON CONFLICT(").AppendString(keyFieldName).AppendString(") DO UPDATE SET ")
		sqlStr.AppendString(versionFieldName).AppendString("=").AppendString(tableMeta.getName()).AppendString(".").AppendString(versionFieldName).AppendString("+1")
		for k, v := range fields {
			sqlStr.AppendString(",").AppendString(k).AppendString("=")
			appendFieldValue2SqlStr(sqlStr, v)
		}

		sqlStr.AppendString(" WHERE ").AppendString(tableMeta.getName()).AppendString(".").AppendString(keyFieldName).AppendString("='").AppendString(t.cmd.key)
		sqlStr.AppendString("' AND ").AppendString(tableMeta.getName()).AppendString(".").AppendString(t.cmd.new.GetName()).AppendString("=")
		appendFieldValue2SqlStr(sqlStr, t.cmd.old).AppendString(";")

		var (
			result  sql.Result
			n       int64
			success = false
		)

		s := sqlStr.ToString()
		start := time.Now()
		result, err = execer.Exec(s)
		getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): insert or update query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err != nil {
			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert or update: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n, err = result.RowsAffected(); err != nil {
			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert or update: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else {

			var (
				fieldMeta     = tableMeta.getFieldMeta(t.cmd.old.GetName())
				valueReceiver = fieldMeta.getReceiver()
			)

			sqlStr.Reset()
			sqlStr.AppendString("SELECT ").AppendString(versionFieldName).AppendString(",").AppendString(t.cmd.new.GetName()).AppendString(" FROM ").AppendString(t.cmd.table).AppendString(" WHERE ")
			sqlStr.AppendString(keyFieldName).AppendString("='").AppendString(t.cmd.key).AppendString("';")

			s = sqlStr.ToString()
			start := time.Now()
			row := queryer.QueryRowx(s)
			getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

			if err = row.Scan(&version, valueReceiver); err != nil {
				if err == sql.ErrNoRows {
					errCode = errcode.ERR_RECORD_NOTEXIST
				} else {
					getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): select: %s.", t.cmd.table, t.cmd.key, err)
					errCode = errcode.ERR_SQLERROR
				}
			} else if n > 0 {
				// 更新成功

				valueField = t.cmd.new
				success = true

			} else {
				// 更新失败

				valueField = proto.PackField(t.cmd.new.GetName(), fieldMeta.getConverter()(valueReceiver))

				if t.cmd.version != nil && version != *t.cmd.version {
					errCode = errcode.ERR_VERSION_MISMATCH
				} else {
					errCode = errcode.ERR_CAS_NOT_EQUAL
				}
			}
		}

		if tx != nil {
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
				valueField = nil
			}
		}
	}

	if sqlStr != nil {
		putStr(sqlStr)
	}
	t.cmd.replyWithValue(errCode, version, valueField)
}

type cmdCompareSetNx struct {
	cmdBase
	version *int64
	old     *proto.Field
	new     *proto.Field
}

func (c *cmdCompareSetNx) makeSqlTask() sqlTask {
	return &sqlTaskCompareSetNx{cmd: c}
}

func (c *cmdCompareSetNx) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.CompareAndSetNxResp{
			Version: version,
		}

		if len(fields) > 0 {
			resp.Value = fields[c.old.GetName()]
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

func (c *cmdCompareSetNx) replyWithValue(errCode int32, version int64, value *proto.Field) {
	if !c.isResponseTimeout() {
		if value != nil {
			c.reply(errCode, version, map[string]*proto.Field{c.old.GetName(): value})
		} else {
			c.reply(errCode, version, nil)
		}
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
