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

//var this *scriptMgr

func (this *scriptMgr) GetCompareAndSetSha() (bool, string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.compareAndSetSha.sha == "" {
		if !this.compareAndSetSha.loading {
			//请求redis加载脚本
			this.compareAndSetSha.loading = true
			go func() {
				this.mtx.Lock()
				defer func() {
					this.compareAndSetSha.loading = false
					this.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(this.compareAndSetSha.script).Result()
				if nil == err {
					this.compareAndSetSha.sha = sha
					Infoln("load compareAndSetSha ok", sha)
				}
			}()
		}
		return false, this.compareAndSetSha.script
	} else {
		return true, this.compareAndSetSha.sha
	}
}

func (this *scriptMgr) GetDelSha() (bool, string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.delSha.sha == "" {
		if !this.delSha.loading {
			//请求redis加载脚本
			this.delSha.loading = true
			go func() {
				this.mtx.Lock()
				defer func() {
					this.delSha.loading = false
					this.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(this.delSha.script).Result()
				if nil == err {
					this.delSha.sha = sha
					Infoln("load delSha ok", sha)
				}
			}()

		}
		return false, this.delSha.script
	} else {
		return true, this.delSha.sha
	}
}

func (this *scriptMgr) GetIncrBySha() (bool, string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.incrBySha.sha == "" {
		if !this.incrBySha.loading {
			//请求redis加载脚本
			this.incrBySha.loading = true
			go func() {
				this.mtx.Lock()
				defer func() {
					this.incrBySha.loading = false
					this.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(this.incrBySha.script).Result()
				if nil == err {
					this.incrBySha.sha = sha
					Infoln("load incrBySha ok", sha)
				}
			}()
		}
		return false, this.incrBySha.script
	} else {
		return true, this.incrBySha.sha
	}
}

func (this *scriptMgr) GetDecrBySha() (bool, string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.decrBySha.sha == "" {
		if !this.decrBySha.loading {
			//请求redis加载脚本
			this.decrBySha.loading = true
			go func() {
				this.mtx.Lock()
				defer func() {
					this.decrBySha.loading = false
					this.mtx.Unlock()
				}()
				sha, err := cli.ScriptLoad(this.decrBySha.script).Result()
				if nil == err {
					this.decrBySha.sha = sha
					Infoln("load decrBySha ok", sha)
				}
			}()
		}
		return false, this.decrBySha.script
	} else {
		return true, this.decrBySha.sha
	}
}

func (this *scriptMgr) LoadSetSha(c int, script string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	s, ok := this.setSha[c]
	load := false
	if !ok {
		s = &scriptSha{
			script: script,
		}
		this.setSha[c] = s
		load = true
	} else if !s.loading {
		load = true
	}

	if load {
		//请求redis加载脚本
		s.loading = true
		go func() {
			this.mtx.Lock()
			defer func() {
				s.loading = false
				this.mtx.Unlock()
			}()
			sha, err := cli.ScriptLoad(s.script).Result()
			if nil == err {
				s.sha = sha
				Infoln("load setsha ok", s.sha)
			}
		}()
	}
}

func (this *scriptMgr) GetSetSha(c int) (string, string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	s, ok := this.setSha[c]
	if !ok {
		return "", ""
	} else {
		return s.script, s.sha
	}
}

//func InitScript() {
func newScriptMgr() *scriptMgr {
	//this = &scriptMgr{
	return &scriptMgr{
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
