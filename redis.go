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


//只有key存在才执行hmset
const strSetExist string = `
	local v = redis.call('hget',KEYS[1],ARGV[1])
	if not v then
		return "not exist"
	else
		redis.call('hmset',KEYS[1],%s)
		return "ok"
	end
`

//只有key存在且版本号一致才执行hmset
const strSetCheckVersion string = `
	local v = redis.call('hget',KEYS[1],ARGV[1])
	if not v then
		return "not exist"
	else if v ~= ARGV[2] then
		return v
	else
		redis.call('hmset',KEYS[1],%s)
		return "ok"
	end
`

const strDelExist string  = `
	local v = redis.call('hget',KEYS[1],ARGV[1])
	if not v then
		return "not exist"
	else
		redis.call('del',KEYS[1])
		return "ok"
	end
`

const strDelCheckVersion string = `
	local v = redis.call('hget',KEYS[1],ARGV[1])
	if not v then
		return "not exist"
	else if v ~= ARGV[2] then
		return v
	else
		redis.call('del',KEYS[1])
		return "ok"
	end
`

var redis_once sync.Once
var cli *redis.Client

var redisProcessQueue []*util.BlockQueue 

func pushRedis(stm *cmdStm) {
	//根据stm.unikey计算hash投递到单独的routine中处理	
	Debugln("pushRedis",stm.uniKey)
	hash := StringHash(stm.uniKey)
	redisProcessQueue[hash%conf.RedisProcessPoolSize].Add(stm)
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


func (this *redisPipeliner) appendSet(stm *cmdStm) interface{} {
	var str string
	ckey := stm.ckey
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
	} else if stm.cmdType == cmdSet {
		cmd.ret = this.appendSet(stm)	
	} else {
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
					cmd.stm.fields[name] = field{
						name  : name,
						value : vv,
					}		
				}
			}
			break
		default:
			panic("error")
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
					cmd.stm.errno = errcode.ERR_NOTFOUND
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
		Debugln("cmdDel error",err1)
		cmd.stm.errno = errcode.ERR_REDIS
	} else {
		if r.(string) == "not exist" {
			cmd.stm.errno = errcode.ERR_NOTFOUND
		} else if r.(string) != "ok" {
			cmd.stm.errno   = errcode.ERR_VERSION
			version,_ := strconv.ParseInt(r.(string), 10, 64)
			cmd.stm.version = new(int64)
			*cmd.stm.version = version
		}
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
				this.readSetResult(v)
			} else if v.stm.cmdType == cmdSet {
				this.readSetResult(v)
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
			redisProcessQueue = make([]*util.BlockQueue,conf.RedisProcessPoolSize)
			for i := 0; i < conf.RedisProcessPoolSize; i++ {
				redisProcessQueue[i] = util.NewBlockQueueWithName(fmt.Sprintf("redis:",i),conf.RedisEventQueueSize)
				go redisRoutine(redisProcessQueue[i])
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
