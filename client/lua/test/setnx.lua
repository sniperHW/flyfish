package.path = './lib/?.lua;./?.lua;./flyfish/?.lua'
package.cpath = './lib/?.so;'

local flyfish = require("flyfish")
local dump = require("dump")

flyfish.init("localhost",10012)

dump.print(flyfish.setNX("users1","huangwei:1015",{flyfish.String("phone","123456")} ))
