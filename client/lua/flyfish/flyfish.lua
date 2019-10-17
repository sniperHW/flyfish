package.path = './lib/?.lua;'
package.cpath = './lib/?.so;'

local chuck = require("chuck")
local protobuf = require "protobuf"
local dump = require("dump")
local event_loop = chuck.event_loop.New()
local readline = require("readline")
local PromiseSocket = require("PromiseSocket").init(event_loop)
local strpack = string.pack
local strunpack = string.unpack

local addr = io.open("flyfish/proto.pb","rb")
local pb_buffer = addr:read "*a"
addr:close()
protobuf.register(pb_buffer)


local M = {
	
}

local CmdToProto = {
	[1] = "proto.ping_req",
	[2] = "proto.ping_resp",
	[3] = "proto.set_req",
	[4] = "proto.set_resp",
	[5] = "proto.get_req",
	[6] = "proto.get_resp",
	[7] = "proto.del_req",
	[8] = "proto.del_resp",
	[9] = "proto.incr_by_req",
	[10] = "proto.incr_by_resp",
	[11] = "proto.decr_by_req",
	[12] = "proto.decr_by_resp",
	[13] = "proto.set_nx_req",
	[14] = "proto.set_nx_resp",
	[15] = "proto.compare_and_set_req",
	[16] = "proto.compare_and_set_resp",	
	[17] = "proto.compare_and_set_nx_req",
	[18] = "proto.compare_and_set_nx_resp",	
	[19] = "proto.kick_req",
	[20] = "proto.kick_resp",
	[21] = "proto.scan_req",
	[22] = "proto.scan_resp",
	[23] = "proto.reloadTableConfReq",
	[24] = "proto.reloadTableConfResp",
	[25] = "proto.reloadConfigReq",
	[26] = "proto.reloadConfigResp",
	[27] = "proto.cancel"	

}


local ProtoToCmd = {}

for k,v in ipairs(CmdToProto) do
	ProtoToCmd[v] = k
end


local errCodeToStr = {
	[0] = "ERR_OK",
	[1] = "ERR_RETRY",
	[2] = "ERR_BUSY",
	[3] = "ERR_VERSION_MISMATCH",
	[4] = "ERR_RECORD_EXIST",
	[5] = "ERR_TIMEOUT",
	[6] = "ERR_SERVER_STOPED",
	[7] = "ERR_SQLERROR",
	[8] = "ERR_NOT_LEADER",
	[9] = "ERR_RAFT",
	[10] = "ERR_SEND_FAILED",
	[11] = "ERR_RECORD_NOTEXIST",
	[12] = "ERR_MISSING_FIELDS",
	[13] = "ERR_MISSING_TABLE",
	[14] = "ERR_MISSING_KEY",
	[15] = "ERR_INVAILD_TABLE",
	[16] = "ERR_INVAILD_FIELD",
	[17] = "ERR_CAS_NOT_EQUAL",
	[18] = "ERR_PROPOSAL_DROPPED",
	[19] = "ERR_CONNECTION",
	[20] = "ERR_OTHER",
}

local function _set_byte1(n)
    return strpack(">I1", n)
end

local function _get_byte1(data, i)
    return strunpack(">I1", data, i)
end

local function _set_byte2(n)
    return strpack(">I2", n)
end

local function _get_byte2(data, i)
    return strunpack(">I2", data, i)
end

local function _set_byte4(n)
    return strpack(">I4", n)
end

local function _get_byte4(data, i)
    return strunpack(">I4", data, i)
end


local function pack(cmd,data) 
	local code = protobuf.encode(CmdToProto[cmd], data)	
	local len = 1 + 2 + #code
	return _set_byte4(len) .. _set_byte1(0) ..  _set_byte2(cmd) .. code 
end

local function unpack(buff)
	_get_byte1(buff,1)
	local cmd = _get_byte2(buff,2)
	return {
		cmd = cmd,
		resp = protobuf.decode(CmdToProto[cmd] , string.sub(buff,4)),
	}
end



local loginReq = protobuf.encode("proto.loginReq", {compress=0})


local function unpackFiled(v)
	if v.v.type == "int" then
		return {type=v.v.type,value=v.v.i}
	elseif v.v.type == "uint" then
		return {type=v.v.type,value=v.v.u}
	elseif v.v.type == "float" then
		return {type=v.v.type,value=v.v.f}
	elseif v.v.type == "string" then
		return {type=v.v.type,value=v.v.s}
	elseif v.v.type == "blob" then
		return {type=v.v.type,value=v.v.b}
	end	
end

local function wrapResult(r)

	if nil == r then
		return r
	end

	local resp = r.resp

	local result = {
		errCode = errCodeToStr[resp.head.errCode],
		key = resp.head.key,
		version = resp.head.version,
	}


	local cmd = CmdToProto[r.cmd]

	if cmd == "proto.compare_and_set_resp" or cmd == "proto.compare_and_set_nx_resp" then
		result.value = unpackFiled(resp.value)
	elseif cmd == "proto.incr_by_resp" or cmd == "proto.decr_by_resp" then
		result.value = unpackFiled(resp.newValue)
	elseif cmd == "proto.get_resp" then
		if resp.fields ~= nil then
			result.fields = {}
			for i, v in ipairs(resp.fields) do
				result.fields[v.name] = unpackFiled(v)					
			end
		end
	end

	return result

end


local function doCmd(cmd)

	local ok
	local c
	local timeout = 5000
	local result 

	PromiseSocket.connect(M.ip,M.port,timeout):andThen(function (conn)
		c = conn
		conn:OnClose(function ()
			ok = true
			c = nil
		end)

		conn:Send(_set_byte2(#loginReq) .. loginReq)
		
		conn:Recv(2,timeout):andThen(function(msg)
			local len = _get_byte2(msg,1)
			return conn:Recv(len,timeout)
		end):andThen(function(msg)
			local loginResp = protobuf.decode("proto.loginResp", string.sub(msg,1))
			conn:Send(pack(cmd.cmd,cmd.req))
			return conn:Recv(4,timeout)
		end):andThen(function(msg)
			local len = _get_byte4(msg,1)
			return conn:Recv(len,timeout)			
		end):andThen(function (msg)
			result = unpack(msg)
			conn:Close()
		end)

	end):catch(function (err)
		print(err)
		ok = true
		if c then
			c:Close()
		end
	end)

	while not ok do
		event_loop:Run(10)
	end

	return wrapResult(result)
	--return dump.print(wrapResult(result),"resp",10)
end


function M.String(name,value)
	return {
		name = name,
		v = {type='string',s=value},
	}	
end

function M.Int(name,value)
	return {
		name = name,
		v = {type='int',i=value},
	}	
end

function M.Uint(name,value)
	return {
		name = name,
		v = {type='uint',u=value},
	}	
end

function M.Float(name,value)
	return {
		name = name,
		v = {type='float',f=value},
	}	
end

function M.Blob(name,value)
	return {
		name = name,
		v = {type='blob',b=value},
	}	
end

--get("users1","huangwei:1015")
function M.get(table,key,fields)
	local cmd = {
		cmd = ProtoToCmd["proto.get_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,
			},
			fields = fields,
		}		
	}

	if cmd.req.fields == nil then
		cmd.req.all = true
	end

	return doCmd(cmd)
end

--set("users1","ak1",{String('name','ak1'))})
--set("game_user","huangwei",{String('userdata','{"Name":"huangwei","Level":2}')})
function M.set(table,key,fields,version)
	local cmd = {
		cmd = ProtoToCmd["proto.set_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			fields = fields,
		}		
	}
	return doCmd(cmd)	
end

function M.setNX(table,key,fields,version)
	local cmd = {
		cmd = ProtoToCmd["proto.set_nx_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			fields = fields,
		}		
	}
	return doCmd(cmd)	
end

function M.compareAndSet(table,key,old,new,version)
	local cmd = {
		cmd = ProtoToCmd["proto.compare_and_set_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			old = old,
			new = new,
		}		
	}
	return doCmd(cmd)	
end


function M.compareAndSetNX(table,key,old,new,version)
	local cmd = {
		cmd = ProtoToCmd["proto.compare_and_set_nx_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			old = old,
			new = new,
		}		
	}
	return doCmd(cmd)
end


function M.del(table,key,version)
	local cmd = {
		cmd = ProtoToCmd["proto.del_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
		}		
	}
	return doCmd(cmd)		
end


function M.kick(table,key)
	local cmd = {
		cmd = ProtoToCmd["proto.kick_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
		}		
	}
	return doCmd(cmd)		
end


function M.incrBy(table,key,v,version)
	local cmd = {
		cmd = ProtoToCmd["proto.incr_by_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			field = v,
		}		
	}
	return doCmd(cmd)
end

function M.decrBy(table,key,v,version)
	local cmd = {
		cmd = ProtoToCmd["proto.decr_by_req"],
		req = {
			head = {
				seqno = 1,
				table = table,
				key = key,
				timeout = 5000000000,
				respTimeout = 10000000000,				
			},
			version = version,
			field = v,
		}		
	}
	return doCmd(cmd)
end


--[[

	[10] = "proto.incr_by_resp",
	[12] = "proto.decr_by_resp",	
	[21] = "proto.scan_req",
	[22] = "proto.scan_resp",
	[23] = "proto.reloadTableConfReq",
	[24] = "proto.reloadTableConfResp",
	[25] = "proto.reloadConfigReq",
	[26] = "proto.reloadConfigResp",
	[27] = "proto.cancel"	

]]

function M.reloadTableConfig()
	local cmd = {
		cmd = ProtoToCmd["proto.reloadTableConfReq"],
		req = {}		
	}
	return doCmd(cmd)		
end

function M.reloadConfig(path)
	local cmd = {
		cmd = ProtoToCmd["proto.reloadConfigReq"],
		req = {path=path}		
	}
	return doCmd(cmd)		
end

function M.init(ip,port)
	M.ip = ip
	M.port = port
end

return M


