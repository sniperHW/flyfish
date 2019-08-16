package flyfish

import (
	"github.com/sniperHW/flyfish/proto"
)

const (
	redis_none       = 0
	redis_get        = 1
	redis_set        = 2 //直接执行set
	redis_del        = 3
	redis_set_script = 4 //执行设置类脚本
	redis_set_only   = 5 //执行set,不需要执行sql回写
	redis_kick       = 6 //剔除cache
	redis_end        = 7
)

const (
	write_back_none   = 0
	write_back_insert = 1
	write_back_update = 2
	write_back_delete = 3
	write_back_force  = 4
)

type processContext struct {
	command       *command
	fields        map[string]*proto.Field
	errno         int32
	writeBackFlag int //回写数据库类型
	ping          bool
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

func (this *processContext) getSetfields() *map[string]interface{} {
	ckey := this.getCacheKey()
	meta := ckey.getMeta()
	ret := map[string]interface{}{}
	for _, v := range meta.fieldMetas {
		vv, ok := this.fields[v.name]
		if ok {
			ret[v.name] = vv.GetValue()
		} else {
			ret[v.name] = v.defaultV
			this.fields[v.name] = proto.PackField(v.name, v.defaultV)
		}
	}
	ret["__version__"] = this.fields["__version__"].GetValue()
	return &ret
}

func (this *processContext) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.command.reply(errCode, fields, version)
}

func (this *cacheKey) setDefaultValue(ctx *processContext) {
	this.values = map[string]*proto.Field{}
	meta := this.getMeta()
	for _, v := range meta.fieldMetas {
		defaultV := proto.PackField(v.name, v.defaultV)
		this.values[v.name] = defaultV
		ctx.fields[v.name] = defaultV
		//Infoln("setDefaultValue", v.name)
	}
}

func (this *cacheKey) setValue(ctx *processContext) {
	this.values = map[string]*proto.Field{}
	for _, v := range ctx.fields {

		Debugln("setValue", v.GetName())

		if !(v.GetName() == "__version__" || v.GetName() == "__key__") {
			this.values[v.GetName()] = v
		}
	}
}

func (this *cacheKey) processClientCmd() {
	this.process_(true)
}

func (this *cacheKey) processQueueCmd() {
	this.process_(false)
}
