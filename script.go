package flyfish

import (
	"sync"
)

//只有key存在且版本号一致才执行hmset
const strSetBeg string = `local v = redis.call('hget',KEYS[1],ARGV[1])
if not v then
return "not_exist"
elseif tonumber(v)~=(ARGV[2]-1) then
return "err_version"
else
redis.call('hmset',KEYS[1]`

const strSetEnd string = `)
return "ok"
end`

//ARGV[1]:filed_name,ARGV[2]:old_value,ARGV[3]:new_value,ARGV[4]:__version__,ARGV[5]:__version__value
const strCompareAndSet string = `local v = redis.call('hmget',KEYS[1],ARGV[4],ARGV[1])
if (not v) or (not v[1]) or (not v[2]) then
return "not_exist"
else
if tonumber(v[1])~=(ARGV[5]-1) then
return "err_version"
end
if 'number'==type(ARGV[2]) then
v[2]=tonumber(v)
end
if v[2]~=ARGV[2] then
return {"failed",v[2]}
else
redis.call('hmset',KEYS[1],ARGV[1],ARGV[3],ARGV[4],ARGV[5])
return {"ok",ARGV[3]}
end
end`

const strDel string = `local v = redis.call('hget',KEYS[1],ARGV[1])
if not v then
return "not_exist"
elseif tonumber(v)~=ARGV[2] then
return "err_version"
else
redis.call('del',KEYS[1])
return "ok"
end`

//ARGV[1]:"__version__",ARGV[2]:__version__value,ARGV[3]:hkey,ARGV[4]:hkey_value

const strIncrBy string = `local r = redis.call('hmget',KEYS[1],ARGV[1],ARGV[3])
if r==nil then
return "not_exist"
end
if r[1]==nil or r[2]==nil then
return "not_exit"
end
if tonumber(r[1])~=(ARGV[2]-1) then
return "err_version"
end
local newVal=r[2]+ARGV[4]
redis.call('hmset',KEYS[1],ARGV[1],ARGV[2],ARGV[3],newVal)
return newVal`

//ARGV[1]:"__version__",ARGV[2]:__version__value,ARGV[3]:hkey,ARGV[4]:hkey_value
const strDecrBy string = `local r = redis.call('hmget',KEYS[1],ARGV[1],ARGV[3])
if r==nil then
return "not_exist"
end
if r[1]==nil or r[2]==nil then
return "not_exit"
end
if tonumber(r[1])~=(ARGV[2]-1) then
return "err_version"
end	
local newVal=r[2]-ARGV[4]
redis.call('hmset',KEYS[1],ARGV[1],ARGV[2],ARGV[3],newVal)
return newVal`

type scriptSha struct {
	sha     string
	script  string
	loading bool
}

type scriptMgr struct {
	mtx              sync.Mutex
	compareAndSetSha scriptSha
	delSha           scriptSha
	incrBySha        scriptSha
	decrBySha        scriptSha
	setSha           map[int]*scriptSha
}

var gScriptMgr *scriptMgr

func GetCompareAndSetSha() (bool, string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	if gScriptMgr.compareAndSetSha.sha == "" {
		if !gScriptMgr.compareAndSetSha.loading {
			//请求redis加载脚本
			gScriptMgr.compareAndSetSha.loading = true
			go func() {
				gScriptMgr.mtx.Lock()
				defer func() {
					gScriptMgr.compareAndSetSha.loading = false
					gScriptMgr.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(gScriptMgr.compareAndSetSha.script).Result()
				if nil == err {
					gScriptMgr.compareAndSetSha.sha = sha
					Infoln("load compareAndSetSha ok", sha)
				}
			}()
		}
		return false, gScriptMgr.compareAndSetSha.script
	} else {
		return true, gScriptMgr.compareAndSetSha.sha
	}
}

func GetDelSha() (bool, string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	if gScriptMgr.delSha.sha == "" {
		if !gScriptMgr.delSha.loading {
			//请求redis加载脚本
			gScriptMgr.delSha.loading = true
			go func() {
				gScriptMgr.mtx.Lock()
				defer func() {
					gScriptMgr.delSha.loading = false
					gScriptMgr.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(gScriptMgr.delSha.script).Result()
				if nil == err {
					gScriptMgr.delSha.sha = sha
					Infoln("load delSha ok", sha)
				}
			}()

		}
		return false, gScriptMgr.delSha.script
	} else {
		return true, gScriptMgr.delSha.sha
	}
}

func GetIncrBySha() (bool, string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	if gScriptMgr.incrBySha.sha == "" {
		if !gScriptMgr.incrBySha.loading {
			//请求redis加载脚本
			gScriptMgr.incrBySha.loading = true
			go func() {
				gScriptMgr.mtx.Lock()
				defer func() {
					gScriptMgr.incrBySha.loading = false
					gScriptMgr.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(gScriptMgr.incrBySha.script).Result()
				if nil == err {
					gScriptMgr.incrBySha.sha = sha
					Infoln("load incrBySha ok", sha)
				}
			}()
		}
		return false, gScriptMgr.incrBySha.script
	} else {
		return true, gScriptMgr.incrBySha.sha
	}
}

func GetDecrBySha() (bool, string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	if gScriptMgr.decrBySha.sha == "" {
		if !gScriptMgr.decrBySha.loading {
			//请求redis加载脚本
			gScriptMgr.decrBySha.loading = true
			go func() {
				gScriptMgr.mtx.Lock()
				defer func() {
					gScriptMgr.decrBySha.loading = false
					gScriptMgr.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(gScriptMgr.decrBySha.script).Result()
				if nil == err {
					gScriptMgr.decrBySha.sha = sha
					Infoln("load decrBySha ok", sha)
				}
			}()
		}
		return false, gScriptMgr.decrBySha.script
	} else {
		return true, gScriptMgr.decrBySha.sha
	}
}

func LoadSetSha(c int, script string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	s, ok := gScriptMgr.setSha[c]
	load := false
	if !ok {
		s = &scriptSha{
			script: script,
		}
		gScriptMgr.setSha[c] = s
		load = true
	} else if !s.loading {
		load = true
	}

	if load {
		//请求redis加载脚本
		s.loading = true
		go func() {
			gScriptMgr.mtx.Lock()
			defer func() {
				s.loading = false
				gScriptMgr.mtx.Unlock()
			}()
			sha, err := cli.ScriptLoad(s.script).Result()
			if nil == err {
				s.sha = sha
				Infoln("load setsha ok", s.sha)
			}
		}()
	}
}

func GetSetSha(c int) (string, string) {
	gScriptMgr.mtx.Lock()
	defer gScriptMgr.mtx.Unlock()
	s, ok := gScriptMgr.setSha[c]
	if !ok {
		return "", ""
	} else {
		return s.script, s.sha
	}
}

func InitScript() {
	gScriptMgr = &scriptMgr{
		compareAndSetSha: scriptSha{
			script: strCompareAndSet,
		},
		delSha: scriptSha{
			script: strDel,
		},
		incrBySha: scriptSha{
			script: strIncrBy,
		},
		decrBySha: scriptSha{
			script: strDecrBy,
		},
		setSha: map[int]*scriptSha{},
	}
}
