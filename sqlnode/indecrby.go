package sqlnode

import (
	"database/sql"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type sqlTaskInDeCrBy struct {
	sqlTaskBase
	delta   *proto.Field
	version *int64
	incr    bool
}

func (t *sqlTaskInDeCrBy) canCombine() bool {
	return false
}

func (t *sqlTaskInDeCrBy) combine(cmd) bool {
	return false
}

func (t *sqlTaskInDeCrBy) do(db *sqlx.DB) (errCode int32, version int64, fields map[string]*proto.Field) {
	var (
		tx  *sqlx.Tx
		err error
	)

	tx, err = db.Beginx()
	if err != nil {
		getLogger().Errorf("task-indecrby: table(%s) key(%s): begin-transaction: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	var (
		sqlStr         = getStr()
		tableMeta      = getDBMeta().getTableMeta(t.table)
		valueName      = t.delta.GetName()
		success        bool
		valueMeta      = tableMeta.getFieldMeta(valueName)
		valueReceiver  = valueMeta.getReceiver()
		s              string
		row            *sqlx.Row
		recordNotExist bool
		curVersion     int64
	)

	defer putStr(sqlStr)
	defer func() {
		if success {
			if err = tx.Commit(); err != nil {
				getLogger().Errorf("task-indecrby: table(%s) key(%s): commit-transaction: %s.", t.table, t.key, err)
				errCode = errcode.ERR_SQLERROR
				version = 0
				fields = nil
			}
		} else {
			if err = tx.Rollback(); err != nil {
				getLogger().Errorf("task-indecrby: table(%s) key(%s): rollback-transaction: %s.", t.table, t.key, err)
			}
		}
	}()

	appendSingleSelectFieldsSqlStr(sqlStr, t.table, t.key, nil, []string{versionFieldName, valueName})

	s = sqlStr.ToString()
	start := time.Now()
	fmt.Println(s)
	row = tx.QueryRowx(s)
	getLogger().Debugf("task-indecrby: table(%s) key(%s): select query:\"%s\" cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

	err = row.Scan(&curVersion, valueReceiver)

	if err == sql.ErrNoRows {
		err = nil
		recordNotExist = true
	} else if err != nil {
		getLogger().Errorf("task-indecrby: table(%s) key(%s): select: %s.", t.table, t.key, err)
		errCode = errcode.ERR_SQLERROR
		return
	}

	var (
		valueDelta = t.delta
		newValue   *proto.Field
		result     sql.Result
		n          int64
	)

	sqlStr.Reset()

	if recordNotExist {
		// 插入

		if t.incr {
			newValue = proto.PackField(valueName, valueMeta.getDefaultV().(int64)+valueDelta.GetInt())
		} else {
			newValue = proto.PackField(valueName, valueMeta.getDefaultV().(int64)-valueDelta.GetInt())
		}

		appendInsertSqlStr(sqlStr, tableMeta, t.key, 1, map[string]*proto.Field{valueName: newValue})

		s = sqlStr.ToString()
		start = time.Now()
		result, err = tx.Exec(s)
		getLogger().Debugf("task-indecrby: table(%s) key(%s): insert query:\"%s\" cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

		if err == nil {
			n, err = result.RowsAffected()
		}

		if err != nil {
			getLogger().Errorf("task-indecrby: table(%s) key(%s): insert: %s.", t.table, t.key, err)
			errCode = errcode.ERR_SQLERROR
			return
		}

		if n <= 0 {
			getLogger().Errorf("task-indecrby: table(%s) key(%s): insert failed - impossible.", t.table, t.key)
			errCode = errcode.ERR_OTHER
			return
		}

		errCode = errcode.ERR_OK
		version = 1
		fields = map[string]*proto.Field{valueName: newValue}
		success = true
		return

	} else {
		// 更新

		var (
			newVersion = curVersion + 1
		)

		if t.incr {
			newValue = proto.PackField(valueName, valueMeta.getConverter()(valueReceiver).(int64)+valueDelta.GetInt())
		} else {
			newValue = proto.PackField(valueName, valueMeta.getConverter()(valueReceiver).(int64)-valueDelta.GetInt())
		}

		if t.version != nil && *t.version != curVersion {
			errCode = errcode.ERR_VERSION_MISMATCH
			return
		}

		appendSingleUpdateSqlStr(sqlStr, t.table, t.key, &curVersion, &newVersion, []*proto.Field{newValue})

		s = sqlStr.ToString()
		start = time.Now()
		result, err = tx.Exec(s)
		getLogger().Debugf("task-indecrby: table(%s) key(%s): update query:\"%s\" cost:%.3f.", t.table, t.key, s, time.Now().Sub(start).Seconds())

		if err == nil {
			n, err = result.RowsAffected()
		}

		if err != nil {
			getLogger().Errorf("task-indecrby: table(%s) key(%s): update: %s.", t.table, t.key, err)
			errCode = errcode.ERR_SQLERROR
			return
		}

		if n <= 0 {
			getLogger().Errorf("task-indecrby: table(%s) key(%s): update failed - impossible.", t.table, t.key)
			errCode = errcode.ERR_OTHER
			return
		}

		errCode = errcode.ERR_OK
		version = newVersion
		fields = map[string]*proto.Field{valueName: newValue}
		success = true

		return
	}

	return
}

type cmdInDeCrBy struct {
	cmdBase
	delta   *proto.Field
	version *int64
	incr    bool
}

func (c *cmdInDeCrBy) canCombine() bool {
	return false
}

func (c *cmdInDeCrBy) makeSqlTask() sqlTask {
	return &sqlTaskInDeCrBy{
		sqlTaskBase: newSqlTaskBase(c.table, c.key),
		delta:       c.delta,
		version:     c.version,
		incr:        c.incr,
	}
}

func (c *cmdInDeCrBy) replyError(errCode int32) {
	c.reply(errCode, 0, nil)
}

func (c *cmdInDeCrBy) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		var resp pb.Message

		var field *proto.Field

		if fields != nil {
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

	processCmd(cmd)
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

	processCmd(cmd)
}
