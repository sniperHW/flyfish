package errcode

const (
	ERR_OK             = int32(0)
	ERR_VERSION        = int32(1) //版本号不匹配
	ERR_NOTFOUND       = int32(2) //数据不存在
	ERR_REDIS          = int32(3) //redis执行错误
	ERR_MISSING_FIELDS = int32(4) //缺少字段
	ERR_MISSING_TABLE  = int32(5) //没有指定表
	ERR_MISSING_KEY    = int32(6) //没有指定key
	ERR_INVAILD_TABLE  = int32(7) //非法表
	ERR_INVAILD_FIELD  = int32(8) //非法字段
	ERR_SEND           = int32(9)
	ERR_TIMEOUT        = int32(10) //超时
	ERR_CLOSE          = int32(11)
	ERR_DISCONNECTED   = int32(12) //连接断开
	ERR_SQLERROR       = int32(13) //SQL语句执行出错
	ERR_LUA_SCRIPT     = int32(14) //lua脚本执行出错
	ERR_STALE_CACHE    = int32(15) //缓存不一致
	ERR_KEY_EXIST      = int32(16) //key已经存在
	ERR_NOT_EQUAL      = int32(17) //指定的field值与要求的不相等
	ERR_SCAN_END       = int32(18)
	ERR_SERVER_STOPED  = int32(19)
	ERR_BUSY           = int32(20)
	ERR_END            = int32(21)
)

var err_str []string = []string{
	"ok",
	"version mismatch",
	"key not found",
	"redis error",
	"arg fields is empty",
	"arg table is empty",
	"arg key is empty",
	"invaild table",
	"invaild fields",
	"send request error",
	"timeout",
	"connection disconnect",
	"connection disconnect",
	"sql error",
	"error on exec lua script",
	"cache stale",
	"key already exist",
	"not equal",
	"scan finish",
	"server stoped",
	"busy",
}

func GetErrorStr(code int32) string {

	if code >= 0 && code < ERR_END {
		return err_str[code]
	} else {
		return "invaild errcode"
	}
}
