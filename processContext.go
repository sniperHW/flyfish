package flyfish

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

type processContext struct {
	command       *command
	fields        map[string]*proto.Field
	errno         int32
	writeBackFlag int //回写数据库类型
	ping          bool
	version       int64
}

func (this *processContext) getCmd() *command {
	return this.command
}

func (this *processContext) getCmdType() int {
	return this.command.cmdType
}

func (this *processContext) getTable() string {
	return this.command.table
}

func (this *processContext) getKey() string {
	return this.command.key
}

func (this *processContext) getUniKey() string {
	return this.command.uniKey
}

func (this *processContext) getCacheKey() *cacheKey {
	return this.command.ckey
}

func (this *processContext) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.command.reply(errCode, fields, version)
}
