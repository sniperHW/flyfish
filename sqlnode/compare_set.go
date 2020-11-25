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
	sqlTaskBase
	oldValue *proto.Field
	newValue *proto.Field
	version  *int64
}

func (t *sqlTaskCompareSet) canCombine() bool {
	return false
}

func (t *sqlTaskCompareSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskCompareSet) do(db *sqlx.DB) (errCode int32, version int64, fields map[string]*proto.Field) {
	var (
		tableMeta = getDBMeta().getTableMeta(t.table)
		tx        *sqlx.Tx
		err       error
	)

	if tx, err = db.Beginx(); err != nil {
		getLogger().Errorf("task-compare-set: table(%s) key(%s): begin-transaction: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	var (
		valueName = t.oldValue.GetName()
		sqlStr    = getStr()
		s         string
		result    sql.Result
		n         int64
		success   = false
	)

	defer putStr(sqlStr)
	defer func() {

		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-commit: %s.", t.table, t.key, err)
			}
		} else {
			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-compare-set: table(%s) key(%s): transaction-rollback: %s.", t.table, t.key, err)
			}
		}

		if err != nil {
			errCode = errcode.ERR_SQLERROR
			version = 0
			fields = nil
		}

	}()

	appendSingleUpdateSqlStr(sqlStr, t.table, t.key, t.version, nil, []*proto.Field{t.newValue}, sqlCond{sqlCondEqual, t.oldValue})

	s = sqlStr.ToString()
	start := time.Now()
	result, err = tx.Exec(s)
	getLogger().Debugf("task-compare-set: table(%s) key(%s): update query:\"%s\" cost:%.3fs.", t.table, t.key, s, time.Now().Sub(start).Seconds())

	if err == nil {
		n, err = result.RowsAffected()
	}

	if err != nil {
		getLogger().Errorf("task-compare-set: table(%s) key(%s): update: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	if n > 0 {
		// 更新成功

		if t.version != nil {
			// 直接计算出新的 version

			errCode = errcode.ERR_OK
			version = *t.version + 1
			fields = map[string]*proto.Field{valueName: t.newValue}
			success = true

			return
		}

		// 查询 version

		sqlStr.Reset()
		appendSingleSelectFieldsSqlStr(sqlStr, t.table, t.key, nil, []string{versionFieldName})

		s = sqlStr.ToString()
		start := time.Now()
		row := tx.QueryRowx(s)
		getLogger().Debugf("task-compare-set: table(%s) key(%s): select version query:\"%s\" cost:%.3fs.", t.table, t.key, s, time.Now().Sub(start).Seconds())

		err = row.Scan(&version)

		if err == sql.ErrNoRows {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): update successfully but record not exist.", t.table, t.key)
			errCode = errcode.ERR_RECORD_NOTEXIST
			return
		}

		if err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): select version: %s.", t.table, t.key, err)
			errCode = errcode.ERR_SQLERROR
			return
		}

		errCode = errcode.ERR_OK
		fields = map[string]*proto.Field{valueName: t.newValue}
		success = true

	} else {
		var (
			fieldMeta     = tableMeta.getFieldMeta(valueName)
			valueReceiver = fieldMeta.getReceiver()
		)

		sqlStr.Reset()
		appendSingleSelectFieldsSqlStr(sqlStr, t.table, t.key, nil, []string{versionFieldName, valueName})

		s = sqlStr.ToString()
		start := time.Now()
		row := db.QueryRowx(s)
		getLogger().Debugf("task-compare-set: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", t.table, t.key, s, time.Now().Sub(start).Seconds())

		err = row.Scan(&version, valueReceiver)

		if err == sql.ErrNoRows {
			errCode = errcode.ERR_RECORD_NOTEXIST
			return
		}

		if err != nil {
			getLogger().Errorf("task-compare-set: table(%s) key(%s): select: %s.", t.table, t.key, err)
			errCode = errcode.ERR_SQLERROR
			return
		}

		fields = map[string]*proto.Field{valueName: proto.PackField(valueName, fieldMeta.getConverter()(valueReceiver))}

		if t.version != nil && version != *t.version {
			errCode = errcode.ERR_VERSION_MISMATCH
		} else {
			errCode = errcode.ERR_CAS_NOT_EQUAL
		}

	}

	return
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
	return &sqlTaskCompareSet{
		sqlTaskBase: newSqlTaskBase(c.table, c.key),
		oldValue:    c.old,
		newValue:    c.new,
		version:     c.version,
	}
}

func (c *cmdCompareSet) replyError(errCode int32) {
	c.reply(errCode, 0, nil)
}

func (c *cmdCompareSet) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		var value *proto.Field

		if fields != nil {
			value = fields[c.old.GetName()]
		}

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

	processCmd(cmd)
}
