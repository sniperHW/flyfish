package flyfish

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
	elseif tonumber(v) ~= (ARGV[2] - 1) then
		return v
	else
		redis.call('hmset',KEYS[1],%s)
		return "ok"
	end
`

//ARGV[1]:filed_name,ARGV[2]:old_value,ARGV[3]:new_value,ARGV[4]:__version__,ARGV[5]:__version__value
const strCompareAndSet string = `
	local v = redis.call('hmget',KEYS[1],ARGV[4],ARGV[1])
	if (not v) or (not v[1]) or (not v[2]) then
		return version
	else

		if tonumber(v[1]) ~= ARGV[5] - 1 then
			return "err_version" .. v[1]
		end

		if 'number' == type(ARGV[2]) then
			v[2] = tonumber(v)
		end

		if v[2] ~= ARGV[2] then
			return v[2]
		else
			redis.call('hmset',KEYS[1],ARGV[1],ARGV[3],ARGV[4],ARGV[5])
			return ARGV[3]
		end
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
	elseif tonumber(v) ~= (ARGV[2] - 1) then
		return v
	else
		redis.call('del',KEYS[1])
		return "ok"
	end
`


//ARGV[1]:"__version__",ARGV[2]:__version__value,ARGV[3]:hkey,ARGV[4]:hkey_value

const strIncrBy string = `
	local r = redis.call('hmget',KEYS[1],ARGV[1],ARGV[3])

	if r == nil then
		return "not exist"
	end

	if r[1] == nil or r[2] == nil then
		return "not exit"
	end

	if tonumber(r[1]) ~= ARGV[2] - 1 then
		return "err_version:" .. r[1]
	end
		
	local newVal = r[2] + ARGV[4]
	redis.call('hmset',KEYS[1],ARGV[1],ARGV[2],ARGV[3],newVal)
	return newVal
`

//ARGV[1]:"__version__",ARGV[2]:__version__value,ARGV[3]:hkey,ARGV[4]:hkey_value
const strDecrBy string = `
	local version,hval = redis.call('hmget',KEYS[1],ARGV[1],ARGV[3])

	if (nil == version) or (nil == hval) then
		return "not exist"
	end

	if tonumber(version) ~= ARGV[2] - 1 then
		return "err_version:" .. version
	end
		
	hval = hval - ARGV[4]
	redis.call('hmset',KEYS[1],ARGV[1],ARGV[2],ARGV[3],hval)
	return hval
`