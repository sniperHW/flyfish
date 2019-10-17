package.path = './lib/?.lua;./?.lua;./flyfish/?.lua'
package.cpath = './lib/?.so;'

local flyfish = require("flyfish")
local dump = require("dump")

flyfish.init("localhost",10012)



dump.print(flyfish.del("users1","aa1"))
dump.print(flyfish.compareAndSetNX("users1","aa1",flyfish.String("phone","123456"),flyfish.String("phone","9")))
dump.print(flyfish.compareAndSetNX("users1","aa1",flyfish.String("phone","123456"),flyfish.String("phone","9")))
dump.print(flyfish.get("users1","aa1"))