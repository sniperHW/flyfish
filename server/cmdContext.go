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
	commands      []*command
	fields        map[string]*proto.Field
	errno         int32
	writeBackFlag int //回写数据库类型
	ping          bool
	version       int64
}

func (this *cmdContext) getCmd() *command {
	return this.commands[0]
}

func (this *cmdContext) getCmdType() int {
	return this.commands[0].cmdType
}

func (this *cmdContext) getTable() string {
	return this.commands[0].table
}

func (this *cmdContext) getKey() string {
	return this.commands[0].key
}

func (this *cmdContext) getUniKey() string {
	return this.commands[0].uniKey
}

func (this *cmdContext) getCacheKey() *cacheKey {
	return this.commands[0].ckey
}

func (this *cmdContext) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	for _, v := range this.commands {
		v.reply(errCode, fields, version)
	}
}

func (this *cmdContext) dontReply() {
	for _, v := range this.commands {
		v.dontReply()
	}
}
