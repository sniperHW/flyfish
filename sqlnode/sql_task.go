package sqlnode

import (
	"github.com/jmoiron/sqlx"
)

type sqlTask interface {
	canCombine() bool
	combine(cmd) bool
	getCommands() []cmd
	do(*sqlx.DB)
	//reply()
}

type sqlTaskBase struct {
	table    string
	key      string
	commands []cmd
}

func newSqlTaskBase(table, key string, commands []cmd) sqlTaskBase {
	return sqlTaskBase{
		table:    table,
		key:      key,
		commands: commands,
	}
}

func (t *sqlTaskBase) getCommands() []cmd {
	return t.commands
}

//func (t *sqlTaskBase) reply(errCode int32, version int64, fields map[string]*proto.Field) {
//	for _, v := range t.commands {
//		v.reply(errCode, version, fields)
//	}
//}

func (t *sqlTaskBase) replyError(errCode int32) {
	for _, v := range t.commands {
		v.replyError(errCode)
	}
}

//type sqlCombinableTaskBase struct {
//	uniKey   string
//	table    string
//	key      string
//	commands []cmd
//	//errCode  int32
//	//fields   map[string]*proto.Field
//	//version  int64
//}
//
//func newSqlCombinableTaskBase(uniKey, table, key string /*, maxFieldCount int*/) sqlCombinableTaskBase {
//	return sqlCombinableTaskBase{
//		uniKey:   uniKey,
//		table:    table,
//		key:      key,
//		commands: make([]cmd, 0, 1),
//		//errCode:  errcode.ERR_OK,
//		//fields:   make(map[string]*proto.Field, maxFieldCount),
//		//version:  0,
//	}
//}
//
//func (t *sqlCombinableTaskBase) canCombine() bool {
//	return true
//}
//
//func (t *sqlCombinableTaskBase) addCmd(cmd cmd) {
//	if cmd == nil {
//		panic("cmd is nil")
//	}
//
//	t.commands = append(t.commands, cmd)
//}
//
//func (t *sqlCombinableTaskBase) reply(errCode int32, version int64, fields map[string]*proto.Field) {
//	for _, cmd := range t.commands {
//		cmd.reply(errCode, version, fields)
//	}
//}
