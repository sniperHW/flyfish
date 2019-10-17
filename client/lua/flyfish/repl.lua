package.path = './lib/?.lua;./?.lua;./flyfish/?.lua'
package.cpath = './lib/?.so;'

local readline = require("readline")
local flyfish = require("flyfish")

local function execute_chunk(str)
	local func,err = load(str)
	if func then
		local ret,err = pcall(func)
		if not ret then
			print("command error:" .. err)
		end
	elseif err then
		print("command error:" .. err)
	end
end

local function repl()
	local chunk = ""

	local prompt = ">>"

	while true do
		local cmd_line = readline(prompt)
		if #cmd_line > 1 then
			if string.byte(cmd_line,#cmd_line) ~= 92 then
				chunk = chunk .. cmd_line
				break
			else
			  	chunk = chunk .. string.sub(cmd_line,1,#cmd_line-1) .. "\n"
				prompt = ">>>"
			end
		else
			break
		end	
	end

	if chunk ~= "" then
		if chunk == "exit" then
			return false
		else
			chunk = [[flyfish = require("flyfish")
					  dump = require("dump")
			]] .. chunk
			execute_chunk(chunk)
		end
	end

	return true
end

if arg == nil or #arg ~= 2 then
	print("useage:lua flyfish.lua ip port")
else
   ip,port = arg[1],arg[2]
   flyfish.init(ip,port)
   while true do
   		if not repl() then
   			return		
   		end	
   end
end