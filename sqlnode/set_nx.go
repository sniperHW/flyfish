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
		tableMeta = getDBMeta().getTableMeta(t.cmd.table)
		sqlStr    = getStr()
		errCode   int32
		version   int64
		fields    map[string]*proto.Field
	)

	appendInsertSqlStr(sqlStr, tableMeta, t.cmd.key, 1, t.cmd.fields)

	s := sqlStr.ToString()
	putStr(sqlStr)

	start := time.Now()
	result, err := db.Exec(s)
	getLogger().Debugf("task-set-nx: table(%s) key(%s): insert query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

	if err == nil {
		if n, err := result.RowsAffected(); err != nil {
			getLogger().Errorf("task-set-nx: table(%s) key(%s): insert: %s.", t.cmd.table, t.cmd.key, err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			errCode = errcode.ERR_OK
			version = 1
		}
	} else {
		getLogger().Debugf("task-set-nx: table(%s) key(%s): insert: %s.", t.cmd.table, t.cmd.key, err)

		sqlStr = getStr()

		var (
			selectAll = len(t.cmd.fields) == tableMeta.getFieldCount()
			fields    map[string]*proto.Field

			queryFieldCount      int
			queryFields          []string
			queryFieldReceivers  []interface{}
			queryFieldConverters []fieldConverter
			getFieldOffset       int
			getFieldCount        int
			versionIndex         int
		)

		if selectAll {
			appendSelectAllSqlStr(sqlStr, tableMeta, t.cmd.key, nil).ToString()

			queryFields = tableMeta.getAllFields()
			queryFieldReceivers = tableMeta.getAllFieldReceivers()
			queryFieldConverters = tableMeta.getAllFieldConverter()
			queryFieldCount = len(queryFields)
			getFieldOffset = 2
			getFieldCount = tableMeta.getFieldCount()
			versionIndex = versionFieldIndex
		} else {
			getFieldCount = len(t.cmd.fields)
			queryFieldCount = getFieldCount + 1
			queryFields = make([]string, queryFieldCount)
			queryFieldReceivers = make([]interface{}, queryFieldCount)
			queryFieldConverters = make([]fieldConverter, queryFieldCount)
			i := 0

			sqlStr.AppendString("select ").AppendString(versionFieldName)
			queryFields[i] = versionFieldName
			queryFieldReceivers[i] = versionFieldMeta.getReceiver()
			queryFieldConverters[i] = versionFieldMeta.getConverter()
			versionIndex = i
			i++
			getFieldOffset = i

			for k, _ := range t.cmd.fields {
				sqlStr.AppendString(",").AppendString(k)
				fm := tableMeta.getFieldMeta(k)
				queryFields[i] = k
				queryFieldReceivers[i] = fm.getReceiver()
				queryFieldConverters[i] = fm.getConverter()
				i++
			}

			sqlStr.AppendString(" from ").AppendString(t.cmd.table).AppendString(" where ").AppendString(keyFieldName)
			sqlStr.AppendString("='").AppendString(t.cmd.key).AppendString("';")
		}

		s = sqlStr.ToString()
		putStr(sqlStr)

		start = time.Now()
		row := db.QueryRowx(s)
		getLogger().Debugf("task-set-nx: table(%s) key(%s): select query:\"%s\" cost:%.3fs.", t.cmd.table, t.cmd.key, s, time.Now().Sub(start).Seconds())

		if err := row.Scan(queryFieldReceivers...); err != nil {
			getLogger().Errorf("task-set-nx: table(%s) key(%s): select: %s.", t.cmd.table, t.cmd.key, err)
			if err != sql.ErrNoRows {
				errCode = errcode.ERR_RECORD_NOTEXIST
			} else {
				errCode = errcode.ERR_SQLERROR
			}
		} else {
			errCode = errcode.ERR_RECORD_EXIST
			version = queryFieldConverters[versionIndex](queryFieldReceivers[versionIndex]).(int64)
			fields = make(map[string]*proto.Field, getFieldCount)

			for i := getFieldOffset; i < queryFieldCount; i++ {
				fieldName := queryFields[i]
				fields[fieldName] = proto.PackField(fieldName, queryFieldConverters[i](queryFieldReceivers[i]))
			}
		}
	}

	t.cmd.reply(errCode, version, fields)
}

type cmdSetNx struct {
	cmdBase
	fields  map[string]*proto.Field
	version *int64
}

func (c *cmdSetNx) makeSqlTask() sqlTask {
	return &sqlTaskSetNx{cmd: c}
}

func (c *cmdSetNx) reply(errCode int32, version int64, fields map[string]*proto.Field) {
	if !c.isResponseTimeout() {
		resp := &proto.SetNxResp{}

		if errCode == errcode.ERR_OK {
			resp.Version = version
		} else if errCode == errcode.ERR_RECORD_EXIST {
			resp.Version = version
			resp.Fields = make([]*proto.Field, len(c.fields))
			tableMeta := getDBMeta().getTableMeta(c.table)
			i := 0
			for k, _ := range c.fields {
				f := fields[k]
				if f != nil {
					resp.Fields[i] = f
				} else {
					// todo impossible in current design.
					resp.Fields[i] = proto.PackField(k, tableMeta.getFieldMeta(k).getDefaultV())
				}
				i++
			}
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
