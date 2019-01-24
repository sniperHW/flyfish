package flyfish

import (
	"flyfish/errcode"
	"flyfish/proto"
	"github.com/go-redis/redis"
)

type redisCmd struct {
	ctx    *processContext
	fields []string
	ret    interface{}
}

var ARGV = []string{
	"ARGV[0]",
	"ARGV[1]",
	",ARGV[2]",
	",ARGV[3]",
	",ARGV[4]",
	",ARGV[5]",
	",ARGV[6]",
	",ARGV[7]",
	",ARGV[8]",
	",ARGV[9]",
	",ARGV[10]",
	",ARGV[11]",
	",ARGV[12]",
	",ARGV[13]",
	",ARGV[14]",
	",ARGV[15]",
	",ARGV[16]",
	",ARGV[17]",
	",ARGV[18]",
	",ARGV[19]",
	",ARGV[20]",
	",ARGV[21]",
	",ARGV[22]",
	",ARGV[23]",
	",ARGV[24]",
	",ARGV[25]",
	",ARGV[26]",
	",ARGV[27]",
	",ARGV[28]",
	",ARGV[29]",
	",ARGV[30]",
	",ARGV[31]",
	",ARGV[32]",
	",ARGV[33]",
	",ARGV[34]",
	",ARGV[35]",
	",ARGV[36]",
	",ARGV[37]",
	",ARGV[38]",
	",ARGV[39]",
	",ARGV[40]",
	",ARGV[41]",
	",ARGV[42]",
	",ARGV[43]",
	",ARGV[44]",
	",ARGV[45]",
	",ARGV[46]",
	",ARGV[47]",
	",ARGV[48]",
	",ARGV[49]",
	",ARGV[50]",
	",ARGV[51]",
	",ARGV[52]",
	",ARGV[53]",
	",ARGV[54]",
	",ARGV[55]",
	",ARGV[56]",
	",ARGV[57]",
	",ARGV[58]",
	",ARGV[59]",
	",ARGV[60]",
	",ARGV[61]",
	",ARGV[62]",
	",ARGV[63]",
	",ARGV[64]",
}

type redisPipeliner struct {
	pipeLiner redis.Pipeliner
	cmds      []*redisCmd
	max       int
	keys      []string
	args      []interface{}
}

func newRedisPipeliner(max int) *redisPipeliner {
	return &redisPipeliner{
		pipeLiner: cli.Pipeline(),
		cmds:      []*redisCmd{},
		max:       max,
		keys:      []string{},
		args:      []interface{}{},
	}
}

func (this *redisPipeliner) appendIncrBy(ctx *processContext) interface{} {
	cmd := ctx.getCmd()
	this.keys = append(this.keys, ctx.getUniKey())
	this.args = append(this.args, "__version__", ctx.fields["__version__"].GetValue(), cmd.incrDecr.GetName(), cmd.incrDecr.GetValue())
	return this.pipeLiner.Eval(strIncrBy, this.keys, this.args...)
}

func (this *redisPipeliner) appendDecrBy(ctx *processContext) interface{} {
	Debugln("appendDecrBy")
	cmd := ctx.getCmd()
	this.keys = append(this.keys, ctx.getUniKey())
	this.args = append(this.args, "__version__", ctx.fields["__version__"].GetValue(), cmd.incrDecr.GetName(), cmd.incrDecr.GetValue())
	return this.pipeLiner.Eval(strDecrBy, this.keys, this.args...)
}

func (this *redisPipeliner) appendCompareAndSet(ctx *processContext) interface{} {
	//ARGV[1]:filed_name,ARGV[2]:old_value,ARGV[3]:new_value,ARGV[4]:__version__,ARGV[5]:__version__value
	cmd := ctx.getCmd()
	this.keys = append(this.keys, ctx.getUniKey())
	this.args = append(this.args, cmd.cns.oldV.GetName(), cmd.cns.oldV.GetValue(), cmd.cns.newV.GetValue(), "__version__", ctx.fields["__version__"].GetValue())
	return this.pipeLiner.Eval(strCompareAndSet, this.keys, this.args...)
}

func (this *redisPipeliner) appendSet(ctx *processContext) interface{} {
	this.keys = append(this.keys, ctx.getUniKey())
	this.args = append(this.args, "__version__", ctx.fields["__version__"].GetValue())
	c := 3
	str := strGet()
	str.append(strSetBeg)
	for _, v := range ctx.fields {
		this.args = append(this.args, v.GetName(), v.GetValue())
		str.append(ARGV[c]).append(ARGV[c+1])
		c += 2
	}
	str.append(strSetEnd)
	ret := this.pipeLiner.Eval(str.toString(), this.keys, this.args...)
	strPut(str)
	return ret
}

func (this *redisPipeliner) readGetResult(rcmd *redisCmd) {
	r, err1 := rcmd.ret.(*redis.SliceCmd).Result()
	if nil != err1 {
		Debugln("readGetResult error", err1)
		rcmd.ctx.errno = errcode.ERR_REDIS
	} else {
		for kk, vv := range r {
			if vv == nil {
				rcmd.ctx.errno = errcode.ERR_STALE_CACHE
				return
			}
			name := rcmd.fields[kk]
			ckey := rcmd.ctx.getCacheKey()
			f := ckey.convertStr(name, vv.(string))
			if nil != f {
				rcmd.ctx.fields[name] = f
			} else {
				Debugln("invaild value", name, vv.(string))
			}
		}
	}
}

func (this *redisPipeliner) readSetResult(rcmd *redisCmd) {
	_, err1 := rcmd.ret.(*redis.StatusCmd).Result()
	if nil != err1 {
		Debugln("readSetResult error", err1)
		rcmd.ctx.errno = errcode.ERR_REDIS
	}
}

func (this *redisPipeliner) readDelResult(rcmd *redisCmd) {
	r, err1 := rcmd.ret.(*redis.Cmd).Result()
	if nil != err1 {
		Debugln("cmdIncr error", err1)
		rcmd.ctx.errno = errcode.ERR_REDIS
	} else {
		if r.(string) != "ok" {
			rcmd.ctx.errno = errcode.ERR_STALE_CACHE
		}
	}
}

func (this *redisPipeliner) readSetScriptResult(rcmd *redisCmd) {
	r, err1 := rcmd.ret.(*redis.Cmd).Result()
	if nil != err1 {
		Debugln("cmdIncr error", err1)
		rcmd.ctx.errno = errcode.ERR_REDIS
	} else {
		cmd := rcmd.ctx.getCmd()
		if cmd.cmdType == cmdSet {
			if r.(string) != "ok" {
				rcmd.ctx.errno = errcode.ERR_STALE_CACHE
			}
		} else if cmd.cmdType == cmdIncrBy || cmd.cmdType == cmdDecrBy {
			switch r.(type) {
			case string:
				rcmd.ctx.errno = errcode.ERR_STALE_CACHE
				break
			case int64:
				rcmd.ctx.fields[cmd.incrDecr.GetName()] = proto.PackField(cmd.incrDecr.GetName(), r.(int64))
				break
			default:
				rcmd.ctx.errno = errcode.ERR_REDIS
				break
			}
		} else if cmd.cmdType == cmdCompareAndSet || cmd.cmdType == cmdCompareAndSetNx {

			switch r.(type) {
			case string:
				rcmd.ctx.errno = errcode.ERR_STALE_CACHE
				break
			case []interface{}:
				vv := r.([]interface{})
				if vv[0].(string) == "failed" {
					rcmd.ctx.errno = errcode.ERR_NOT_EQUAL
				}
				rcmd.ctx.fields[cmd.cns.oldV.GetName()] = cmd.ckey.convertStr(cmd.cns.oldV.GetName(), vv[1].(string))
				break
			default:
				rcmd.ctx.errno = errcode.ERR_REDIS
				break
			}
		}
	}
}

func (this *redisPipeliner) append(ctx *processContext) {
	rcmd := &redisCmd{
		ctx: ctx,
	}

	if ctx.redisFlag == redis_set || ctx.redisFlag == redis_set_only {
		Debugln("append set", ctx.redisFlag)
		rcmd.ret = this.pipeLiner.HMSet(ctx.getUniKey(), *ctx.getSetfields())
	} else if ctx.redisFlag == redis_get {
		rcmd.fields = make([]string, len(ctx.fields))
		c := 0
		for k, _ := range ctx.fields {
			rcmd.fields[c] = k
			c++
		}
		rcmd.ret = this.pipeLiner.HMGet(ctx.getUniKey(), rcmd.fields...)
	} else if ctx.redisFlag == redis_del {
		this.keys = this.keys[0:0]
		this.args = this.args[0:0]
		this.keys = append(this.keys, ctx.getUniKey())
		this.args = append(this.args, "__version__", ctx.fields["__version__"].GetValue())
		rcmd.ret = this.pipeLiner.Eval(strDel, this.keys, this.args...)
	} else if ctx.redisFlag == redis_set_script {
		cmdType := ctx.getCmdType()

		this.keys = this.keys[0:0]
		this.args = this.args[0:0]

		if cmdType == cmdCompareAndSet || cmdType == cmdCompareAndSetNx {
			rcmd.ret = this.appendCompareAndSet(ctx)
		} else if cmdType == cmdSet {
			rcmd.ret = this.appendSet(ctx)
		} else if cmdType == cmdIncrBy {
			rcmd.ret = this.appendIncrBy(ctx)
		} else if cmdType == cmdDecrBy {
			rcmd.ret = this.appendDecrBy(ctx)
		} else {
			panic("invaild cmdType")
		}
	} else {
		panic("invaild redisFlag")
	}

	this.cmds = append(this.cmds, rcmd)

	if len(this.cmds) >= this.max {
		this.exec()
	}
}

func (this *redisPipeliner) exec() {
	if len(this.cmds) == 0 {
		return
	}
	_, err := this.pipeLiner.Exec()
	for _, v := range this.cmds {
		v.ctx.errno = errcode.ERR_OK
		if nil != err {
			v.ctx.errno = errcode.ERR_REDIS
			Errorln("redis exec error", err)
		} else {
			if v.ctx.redisFlag == redis_get {
				this.readGetResult(v)
			} else if v.ctx.redisFlag == redis_set || v.ctx.redisFlag == redis_set_only {
				this.readSetResult(v)
			} else if v.ctx.redisFlag == redis_del {
				this.readDelResult(v)
			} else {
				this.readSetScriptResult(v)
			}
		}
		onRedisResp(v.ctx)
	}
	this.cmds = this.cmds[0:0]
}
