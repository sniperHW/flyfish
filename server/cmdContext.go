package server

import (
	"github.com/sniperHW/flyfish/proto"
)

const (
	write_back_none          = 0
	write_back_insert        = 1
	write_back_update        = 2
	write_back_delete        = 3
	write_back_insert_update = 4
)

type cmdContext struct {
	command       *command
	fields        map[string]*proto.Field
	errno         int32
	writeBackFlag int //回写数据库类型
	ping          bool
	version       int64
}

func (this *cmdContext) getCmd() *command {
	return this.command
}

func (this *cmdContext) getCmdType() int {
	return this.command.cmdType
}

func (this *cmdContext) getTable() string {
	return this.command.table
}

func (this *cmdContext) getKey() string {
	return this.command.key
}

func (this *cmdContext) getUniKey() string {
	return this.command.uniKey
}

func (this *cmdContext) getCacheKey() *cacheKey {
	return this.command.ckey
}

func (this *cmdContext) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.command.reply(errCode, fields, version)
}
