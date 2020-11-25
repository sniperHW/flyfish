package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/proto"
)

type sqlTask interface {
	canCombine() bool
	combine(cmd) bool
	do(*sqlx.DB) (errCode int32, version int64, fields map[string]*proto.Field)
}

type sqlTaskBase struct {
	table string
	key   string
}

func newSqlTaskBase(table, key string) sqlTaskBase {
	return sqlTaskBase{
		table: table,
		key:   key,
	}
}
