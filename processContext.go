package flyfish

import (
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
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
	commands      []*command
	fields        map[string]*proto.Field
	errno         int32
	replyed       int32 //是否已经应道
	writeBackFlag int   //回写数据库类型
	redisFlag     int
	ping          bool
	replyOnDbOk   bool //是否在db操作完成后才返回响应
}

func (this *processContext) getCmd() *command {
	return this.commands[0]
}

func (this *processContext) getCmdType() int {
	return this.getCmd().cmdType
}

func (this *processContext) getTable() string {
	return this.getCmd().table
}

func (this *processContext) getKey() string {
	return this.getCmd().key
}

func (this *processContext) getUniKey() string {
	return this.getCmd().uniKey
}

func (this *processContext) getCacheKey() *cacheKey {

	if this.getCmd() == nil {
		panic("command == nil")
	}

	if this.getCmd().ckey == nil {
		panic("ckey == nil")
	}

	return this.getCmd().ckey
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
	if atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		for _, v := range this.commands {
			v.reply(errCode, fields, version)
		}
	}
}

func (this *cacheKey) setDefaultValue(ctx *processContext) {
	this.values = map[string]*proto.Field{}
	meta := this.getMeta()
	for _, v := range meta.fieldMetas {
		defaultV := proto.PackField(v.name, v.defaultV)
		this.values[v.name] = defaultV
		ctx.fields[v.name] = defaultV
	}
}

func (this *cacheKey) setValue(ctx *processContext) {
	this.values = map[string]*proto.Field{}
	for _, v := range ctx.fields {
		if !(v.GetName() != "__version__" || v.GetName() != "__key__") {
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
