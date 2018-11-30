package errcode

const (
	ERR_OK 							= int32(0)
	ERR_VERSION 					= int32(1)  //版本号不匹配
	ERR_NOTFOUND 					= int32(2)  //数据不存在
	ERR_REDIS    					= int32(3)  //redis执行错误
	ERR_CMD_MISSING_FIELDS      	= int32(4)
	ERR_CMD_MISSING_TABLE           = int32(5)
	ERR_CMD_MISSING_KEY             = int32(6)
	ERR_INVAILD_TABLE               = int32(7)
	ERR_INVAILD_FIELD               = int32(8)
	ERR_SEND                        = int32(9)
	ERR_TIMEOUT                     = int32(10)
	ERR_CLOSE                       = int32(11)
	ERR_DISCONNECTED                = int32(12)
	ERR_SQLERROR                    = int32(13)
)