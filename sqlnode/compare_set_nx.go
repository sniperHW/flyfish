package sqlnode

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskCompareSetNx struct {
	sqlTaskBase
	oldValue *proto.Field
	newValue *proto.Field
	version  *int64
}

func (t *sqlTaskCompareSetNx) canCombine() bool {
	return false
}

func (t *sqlTaskCompareSetNx) combine(cmd) bool {
	return false
}

func (t *sqlTaskCompareSetNx) do(db *sqlx.DB) (errCode int32, version int64, fields map[string]*proto.Field) {
	var (
		tableMeta = getDBMeta().getTableMeta(t.table)
		valueName = t.oldValue.GetName()
		tx        *sqlx.Tx
		err       error
	)

	if tx, err = db.Beginx(); err != nil {
		getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): begin transaction: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return

	}

	var (
		sqlStr        = getStr()
		curVersion    int64
		fieldMeta     = tableMeta.getFieldMeta(valueName)
		valueReceiver = fieldMeta.getReceiver()
		success       = false
		s             string
		row           *sqlx.Row
		result        sql.Result
		n             int64
	)

	defer putStr(sqlStr)
	defer func() {

		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): transaction-commit: %s.", t.table, t.key, err)
			}
		} else {
			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): transaction-rollback: %s.", t.table, t.key, err)
			}
		}

		if err != nil {
			errCode = errcode.ERR_SQLERROR
			version = 0
			fields = nil
		}

	}()

	appendSingleSelectFieldsSqlStr(sqlStr, t.table, t.key, nil, []string{versionFieldName, valueName})
	s = sqlStr.ToString()
	start := time.Now()
	row = tx.QueryRowx(s)
	getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): select query:\"%s\" cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

	err = row.Scan(&curVersion, valueReceiver)

	if err == sql.ErrNoRows {
		// 记录不存在，插入新记录

		sqlStr.Reset()
		appendInsertSqlStr(sqlStr, tableMeta, t.key, 1, map[string]*proto.Field{valueName: t.newValue})

		s = sqlStr.ToString()
		start = time.Now()
		result, err = tx.Exec(s)
		getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): insert query:\"%s\" cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

		if err == nil {
			n, err = result.RowsAffected()
		}

		if err != nil {
			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert: %s.", t.table, t.key, err)
			errCode = errcode.ERR_SQLERROR
			return
		}

		if n <= 0 {
			// 插入失败, impossible

			getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): insert failed - impossible.", t.table, t.key)
			errCode = errcode.ERR_OTHER
			return
		}

		// 插入成功
		errCode = errcode.ERR_OK
		version = 1
		fields = map[string]*proto.Field{valueName: t.newValue}
		success = true

		return
	}

	if err != nil {
		getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): select: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	// 记录存在

	var (
		curValue = proto.PackField(valueName, fieldMeta.getConverter()(valueReceiver))
	)

	if t.version != nil && *t.version != curVersion {
		// 版本不匹配

		errCode = errcode.ERR_VERSION_MISMATCH
		version = curVersion
		fields = map[string]*proto.Field{valueName: curValue}
		return

	}

	if !curValue.IsEqual(t.oldValue) {
		// 值不匹配

		errCode = errcode.ERR_CAS_NOT_EQUAL
		version = curVersion
		fields = map[string]*proto.Field{valueName: curValue}
		return

	}

	// 更新 value

	var (
		newVersion = curVersion + 1
	)

	sqlStr.Reset()
	appendSingleUpdateSqlStr(sqlStr, t.table, t.key, &curVersion, &newVersion, []*proto.Field{t.newValue})

	s = sqlStr.ToString()
	start = time.Now()
	result, err = tx.Exec(s)
	getLogger().Debugf("task-compare-set-nx: table(%s) key(%s): update query: %s cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

	if err == nil {
		n, err = result.RowsAffected()
	}

	if err != nil {
		getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): update: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	if n <= 0 {
		// 更新失败，理论上 impossible

		getLogger().Errorf("task-compare-set-nx: table(%s) key(%s): update failed - impossible.", t.table, t.key)
		errCode = errcode.ERR_OTHER
		return

	}

	// 更新成功
	errCode = errcode.ERR_OK
	version = newVersion
	fields = map[string]*proto.Field{valueName: t.newValue}
	success = true

	return
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
	return &sqlTaskCompareSetNx{
		sqlTaskBase: newSqlTaskBase(c.table, c.key),
		oldValue:    c.old,
		newValue:    c.new,
		version:     c.version,
	}
}

func (c *cmdCompareSetNx) replyError(errCode int32) {
	c.reply(errCode, 0, nil)
}

func (c *cmdCompareSetNx) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		var value *proto.Field

		if fields != nil {
			value = fields[c.old.GetName()]
		}

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

	processCmd(cmd)
}
