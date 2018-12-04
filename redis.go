package flyfish

import(
	"github.com/sniperHW/kendynet/util"
	"github.com/go-redis/redis"
	"sync"
	"strconv"
	"fmt"
	"strings"
	"flyfish/conf"
	"flyfish/errcode"
)

var redis_once sync.Once
var cli *redis.Client

var redisProcessQueue *util.BlockQueue

func pushRedis(stm *cmdStm) {
	Debugln("pushRedis",stm.uniKey)
	redisProcessQueue.AddWait(stm)
}

type redisCmd struct {
	stm     *cmdStm
	fields  []string
	ret     interface{} 
} 

type redisPipeliner struct {
	pipeLiner redis.Pipeliner
	cmds      []*redisCmd
	max       int
}

func newRedisPipeliner(max int) *redisPipeliner {
	return &redisPipeliner{
		pipeLiner : cli.Pipeline(),
		cmds      : []*redisCmd{},
		max       : max,
	} 	
}

func (this *redisPipeliner) appendIncrBy(stm *cmdStm) interface{} {
	ckey := stm.ckey
	if ckey.status == cache_new || ckey.status == cache_missing {
		//缓存中没有，直接设置
		setFields := map[string]interface{}{}
		for _,v := range(stm.fields) {
			if v.name == stm.wantField.name {
				oldV := v.value.(int64)
				newV := oldV + stm.wantField.value.(int64)
				setFields[v.name] = newV
				stm.fields[v.name] = field{
					name : v.name,
					value : newV,
				}
			} else {
				setFields[v.name] = v.value
			}
		}
		return this.pipeLiner.HMSet(stm.uniKey,setFields)		
	} else {

		keys := []string{stm.uniKey}
		args := []interface{}{"__version__",stm.fields["__version__"].value,stm.wantField.name,stm.wantField.value}
		return this.pipeLiner.Eval(strIncrBy,keys,args...)
	}
}

func (this *redisPipeliner) appendDecrBy(stm *cmdStm) interface{} {
	ckey := stm.ckey
	if ckey.status == cache_new || ckey.status == cache_missing {
		//缓存中没有，直接设置
		setFields := map[string]interface{}{}
		for _,v := range(stm.fields) {
			if v.name == stm.wantField.name {
				oldV := v.value.(int64)
				newV := oldV - stm.wantField.value.(int64)
				setFields[v.name] = newV
				stm.fields[v.name] = field{
					name : v.name,
					value : newV,
				}
			} else {
				setFields[v.name] = v.value
			}
		}
		return this.pipeLiner.HMSet(stm.uniKey,setFields)		
	} else {
		keys := []string{stm.uniKey}
		args := []interface{}{"__version__",stm.fields["__version__"].value,stm.wantField.name,stm.wantField.value}
		return this.pipeLiner.Eval(strDecrBy,keys,args...)
	}
}

func (this *redisPipeliner) appendDel(stm *cmdStm) interface{} {
	var str string
	keys := []string{stm.uniKey}
	args := []interface{}{"__version__"}
	if nil != stm.version {
		str = strDelCheckVersion
		args = append(args,*stm.version)
	} else {
		str = strDelExist
	}
	return this.pipeLiner.Eval(str,keys,args...)
}

func (this *redisPipeliner) appendCompareAndSet(stm *cmdStm) interface{} {
	ckey := stm.ckey
	fmt.Println(ckey.status)
	if ckey.status == cache_new || ckey.status == cache_missing {
		setFields := map[string]interface{}{}
		for _,v := range(stm.fields) {
			setFields[v.name] = v.value
		}
		return this.pipeLiner.HMSet(stm.uniKey,setFields)		
	} else {

		//strCompareAndSet
		//ARGV[1]:filed_name,ARGV[2]:old_value,ARGV[3]:new_value,ARGV[4]:__version__,ARGV[5]:__version__value
		keys := []string{stm.uniKey}
		args := []interface{}{}
		args = append(args,stm.oldV.name,stm.oldV.value,stm.newV.value,"__version__",stm.fields["__version__"].value)
		fmt.Println(args)
		return this.pipeLiner.Eval(strCompareAndSet,keys,args...)
	}	
}

func (this *redisPipeliner) appendSet(stm *cmdStm) interface{} {
	var str string
	ckey := stm.ckey
	fmt.Println(ckey.status)
	if ckey.status == cache_new || ckey.status == cache_missing {
		setFields := map[string]interface{}{}
		for _,v := range(stm.fields) {
			setFields[v.name] = v.value
		}
		return this.pipeLiner.HMSet(stm.uniKey,setFields)		
	} else {
		keys := []string{stm.uniKey}
		args := []interface{}{"__version__"}
		ARGV := []string{}
		if nil != stm.version {
			str = strSetCheckVersion
			c := 3
			args = append(args,*stm.version)
			for _,v := range(stm.fields) {
				args = append(args,v.name,v.value)
				ARGV = append(ARGV,fmt.Sprintf("ARGV[%d]",c),fmt.Sprintf("ARGV[%d]",c + 1))
				c += 2					
			}
		} else {
			str = strSetExist
			c := 2
			for _,v := range(stm.fields) {
				args = append(args,v.name,v.value)
				ARGV = append(ARGV,fmt.Sprintf("ARGV[%d]",c),fmt.Sprintf("ARGV[%d]",c + 1))
				c += 2					
			}
		}
		return this.pipeLiner.Eval(fmt.Sprintf(str,strings.Join(ARGV,",")),keys,args...)
	}
}

func (this *redisPipeliner) append(stm *cmdStm) {
	cmd := &redisCmd {
		stm : stm,
	}

	if stm.setRedisOnly {
		setFields := map[string]interface{}{}
		for _,v := range(stm.fields) {
			setFields[v.name] = v.value
		}
		cmd.ret = this.pipeLiner.HMSet(stm.uniKey,setFields)
	} else if stm.cmdType == cmdGet {
		cmd.fields = make([]string,len(stm.fields))
		c := 0
		for k,_ := range(stm.fields) {
			cmd.fields[c] = k
			c++
		}
		cmd.ret = this.pipeLiner.HMGet(stm.uniKey,cmd.fields...)
	} else if stm.cmdType == cmdSet || stm.cmdType == cmdSetNx {
		cmd.ret = this.appendSet(stm)	
	} else if stm.cmdType == cmdCompareAndSet || stm.cmdType == cmdCompareAndSetNx  {
		cmd.ret = this.appendCompareAndSet(stm)
	} else if stm.cmdType == cmdIncrBy {
		cmd.ret = this.appendIncrBy(stm)
	} else if stm.cmdType == cmdDecrBy {
		cmd.ret = this.appendDecrBy(stm)
	}else {
		cmd.ret = this.appendDel(stm)		
	}
	this.cmds = append(this.cmds,cmd)

	if len(this.cmds) >= this.max {
		this.exec()
	}
}

func (this *redisPipeliner) readGetResult(cmd *redisCmd) {
	switch cmd.ret.(type) {
		case *redis.StatusCmd:
			_,err1 := cmd.ret.(*redis.StatusCmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			} else {
				cmd.stm.version = new(int64)
				*cmd.stm.version = cmd.stm.fields["__version__"].value.(int64)
			}
			break
		case *redis.SliceCmd:
			r,err1 := cmd.ret.(*redis.SliceCmd).Result()
			if nil != err1 {
				Debugln("cmdGet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			} else{
				for kk,vv := range(r) {
					name := cmd.fields[kk]
					ckey := cmd.stm.ckey
					f := ckey.convertStr(name,vv.(string))
					if nil != f {
						cmd.stm.fields[name] = *f
					}
				}
				fmt.Println(cmd.stm.fields)
			}
			break
		default:
			panic("error")
	}	
}

func (this *redisPipeliner) readCompareAndSet(cmd *redisCmd) {
	fmt.Println("readCompareAndSet")
	switch cmd.ret.(type) {
		case *redis.StatusCmd:
			_,err1 := cmd.ret.(*redis.StatusCmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			}
			break
		case *redis.Cmd:
			r,err1 := cmd.ret.(*redis.Cmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			} else {
				if r.(string) == "not exist" || r.(string) == "err_version" {
					cmd.stm.errno = errcode.ERR_STALE_CACHE
				} else {
					f := cmd.stm.ckey.convertStr(cmd.stm.newV.name,r.(string))
					if nil == f {
						fmt.Println(r.(string),cmd.stm.newV.name)
						cmd.stm.errno = errcode.ERR_REDIS
					} else if !cmd.stm.newV.Equal(f) {
						cmd.stm.errno = errcode.ERR_NOT_EQUAL
						cmd.stm.fields[cmd.stm.newV.name] = *f
					} else {
						cmd.stm.fields[cmd.stm.newV.name] = cmd.stm.newV
					}
				}
			}
			break
		default:
			panic("error")
	}

	if cmd.stm.errno == errcode.ERR_OK {
		cmd.stm.version = new(int64)
		*cmd.stm.version = cmd.stm.fields["__version__"].value.(int64)
	}
}

func (this *redisPipeliner) readSetResult(cmd *redisCmd) {
	switch cmd.ret.(type) {
		case *redis.StatusCmd:
			_,err1 := cmd.ret.(*redis.StatusCmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			}
			break
		case *redis.Cmd:
			r,err1 := cmd.ret.(*redis.Cmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			} else {
				if r.(string) == "not exist" {
					cmd.stm.errno = errcode.ERR_STALE_CACHE
				} else if r.(string) != "ok" {
					cmd.stm.errno   = errcode.ERR_VERSION
					version,_ := strconv.ParseInt(r.(string), 10, 64)
					cmd.stm.version = new(int64)
					*cmd.stm.version = version
				}
			}
			break
		default:
			panic("error")
	}

	if cmd.stm.errno == errcode.ERR_OK {
		cmd.stm.version = new(int64)
		*cmd.stm.version = cmd.stm.fields["__version__"].value.(int64)
	}	
}

func (this *redisPipeliner) readDelResult(cmd *redisCmd) {
	r,err1 := cmd.ret.(*redis.Cmd).Result()
	if nil != err1 {
		Debugln("cmdIncr error",err1)
		cmd.stm.errno = errcode.ERR_REDIS
	} else {
		if r.(string) == "not exist" {
			cmd.stm.errno = errcode.ERR_STALE_CACHE
		} else {
			cmd.stm.errno   = errcode.ERR_VERSION
			version,_ := strconv.ParseInt(r.(string), 10, 64)
			cmd.stm.version = new(int64)
			*cmd.stm.version = version
		}
	}
}

func (this *redisPipeliner) readIncrDecrResult(cmd *redisCmd) {

	switch cmd.ret.(type) {
		case *redis.StatusCmd:
			_,err1 := cmd.ret.(*redis.StatusCmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			}
			break
		case *redis.Cmd:
			r,err1 := cmd.ret.(*redis.Cmd).Result()
			if nil != err1 {
				Debugln("cmdSet error",err1)
				cmd.stm.errno = errcode.ERR_REDIS
			} else {
				
				switch r.(type) {
				case string:
					if r.(string) == "not exist" {
						cmd.stm.errno = errcode.ERR_STALE_CACHE
					} else {
						cmd.stm.errno = errcode.ERR_VERSION
						v := strings.Split(r.(string),":")
						version,_ := strconv.ParseInt(v[1], 10, 64)
						cmd.stm.version = new(int64)
						*cmd.stm.version = version						
					}
					break
				case int64:
					cmd.stm.fields[cmd.stm.wantField.name] = field {
						name : cmd.stm.wantField.name,
						value : r.(int64),
					}
					break
				default:
					cmd.stm.errno = errcode.ERR_LUA_SCRIPT
					break
				}
			}
			break
		default:
			panic("error")
	}

	if cmd.stm.errno == errcode.ERR_OK {
		cmd.stm.version = new(int64)
		*cmd.stm.version = cmd.stm.fields["__version__"].value.(int64)
	}
}

func (this *redisPipeliner) exec() {
	if len(this.cmds) == 0 {
		return
	}
	_ , err := this.pipeLiner.Exec()
	for _,v := range(this.cmds) {
		v.stm.errno = errcode.ERR_OK
		if nil != err {
			v.stm.errno = errcode.ERR_REDIS
			Errorln("redis exec error",err)
		} else {
			if v.stm.cmdType == cmdGet {
				this.readGetResult(v)
			} else if v.stm.cmdType == cmdSet || v.stm.cmdType == cmdSetNx {
				this.readSetResult(v)
			} else if v.stm.cmdType == cmdCompareAndSet || v.stm.cmdType == cmdCompareAndSetNx {
				this.readCompareAndSet(v)
			} else if v.stm.cmdType == cmdIncrBy {
				this.readIncrDecrResult(v)
			} else if v.stm.cmdType == cmdDecrBy {
				this.readIncrDecrResult(v)
			} else {
				this.readDelResult(v)
			}
		}
		onRedisResp(v.stm)
	}
	this.cmds = []*redisCmd{}
} 


func redisRoutine(queue *util.BlockQueue) {
	redisPipeliner_ := newRedisPipeliner(conf.RedisPipelineSize)
	for {
		closed, localList := queue.Get()	
		for _,v := range(localList) {
			stm := v.(*cmdStm)
			redisPipeliner_.append(stm)
		}
		redisPipeliner_.exec()
		if closed {
			return
		}
	}	
}

func RedisClose() {
	
}

func RedisInit(Addr string,Password string) bool {
	redis_once.Do(func() {
		cli = redis.NewClient(&redis.Options{
			Addr:     Addr,
			Password: Password,
		})

		if nil != cli {
			redisProcessQueue = util.NewBlockQueueWithName(fmt.Sprintf("redis"),conf.RedisEventQueueSize)
			for i := 0; i < conf.RedisProcessPoolSize; i++ {
				go redisRoutine(redisProcessQueue)
			}

			/*go func(){
				for {
					time.Sleep(time.Second)
					fmt.Println("---------------redisProcessQueue-------------")
					for _,v := range(redisProcessQueue) {
						fmt.Println(v.Len())
					}
				}
			}()*/		
		}
	})
	return cli != nil
}
