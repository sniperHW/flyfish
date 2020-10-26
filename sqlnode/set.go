package sqlnode

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"strings"
)

type sqlTaskSet struct {
	sqlTaskBase
	cmd *cmdSet
}

func (t *sqlTaskSet) combine(cmd) bool {
	return false
}

func (t *sqlTaskSet) do(db *sqlx.DB) {
	tableMeta := getDBMeta().getTableMeta(t.table)

	var (
		errCode int32
		version int64
		sqlStr  = getStr()
	)

	if t.cmd.version != nil {
		// update 'table' set
		sqlStr.AppendString("update ").AppendString(t.table).AppendString(" set")

		// field_name=field_value,...,field_name=field_value
		sqlStr.AppendString(" ").AppendString(t.cmd.fields[0].GetName()).AppendString("=")
		appendFieldValue2SqlStr(sqlStr, t.cmd.fields[0])
		for i := 1; i < len(t.cmd.fields); i++ {
			sqlStr.AppendString(",").AppendString(t.cmd.fields[0].GetName()).AppendString("=")
			appendFieldValue2SqlStr(sqlStr, t.cmd.fields[0])
		}
		sqlStr.AppendString(" ")

		// where __key__='key' and __version__='version'
		sqlStr.AppendString("where ").AppendString(keyFieldName).AppendString("=").AppendString("'").AppendString(t.table).AppendString("' ")
		sqlStr.AppendString("and ").AppendString(versionFieldName).AppendString("=")
		appendValue2SqlStr(sqlStr, proto.ValueType_int, *t.cmd.version).AppendString(";")

		s := sqlStr.ToString()

		getLogger().Debugf("task-set: table(%s) key(%d): start query: %s.", t.table, t.key, s)

		if result, err := db.Exec(s); err != nil {
			getLogger().Debugf("task-set: table(%s) key(%d): %s.", err)
			errCode = errcode.ERR_SQLERROR
		} else if n, err := result.RowsAffected(); err != nil {
			getLogger().Debugf("task-set: table(%s) key(%d): %s.", err)
			errCode = errcode.ERR_SQLERROR
		} else if n > 0 {
			errCode = errcode.ERR_OK
			version = *t.cmd.version
		} else {
			errCode = errcode.ERR_VERSION_MISMATCH
		}

		t.reply(errCode, nil, version)
	} else {
		appendInsertOrUpdateSqlStr(sqlStr, t.table, t.key, t.cmd.fields)
	}

}

type cmdSet struct {
	cmdBase
	fields  []*proto.Field
	version *int64
}

func (c *cmdSet) makeSqlTask() sqlTask {
	panic("implement me")
}

func (c *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	panic("implement me")
}

func onSet(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.SetReq)
}
