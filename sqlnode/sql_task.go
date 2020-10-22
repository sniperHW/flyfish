package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/proto"
	"sanguo/flyfish/errcode"
)

type sqlTask interface {
	combine(cmd) bool
	do(*sqlx.DB)
	reply()
}

type sqlTaskBase struct {
	uniKey   string
	table    string
	key      string
	commands []cmd
	errCode  int32
	fields   map[string]*proto.Field
	version  int64
}

func newSqlTaskBase(uniKey, table, key string, maxFieldCount int) sqlTaskBase {
	return sqlTaskBase{
		uniKey:   uniKey,
		table:    table,
		key:      key,
		commands: make([]cmd, 0, 1),
		errCode:  errcode.ERR_OK,
		fields:   make(map[string]*proto.Field, maxFieldCount),
		version:  0,
	}
}

func (t *sqlTaskBase) addCmd(cmd cmd) {
	if cmd == nil {
		panic("cmd is nil")
	}

	t.commands = append(t.commands, cmd)
}

func (t *sqlTaskBase) reply() {
	for _, cmd := range t.commands {
		cmd.reply(t.errCode, t.fields, t.version)
	}
}
