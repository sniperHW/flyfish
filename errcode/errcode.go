package errcode

const (
	ERR_OK = int32(iota)
	ERR_RETRY
	ERR_BUSY
	ERR_VERSION_MISMATCH
	ERR_RECORD_EXIST //key已经存在
	ERR_TIMEOUT
	ERR_SERVER_STOPED
	ERR_SQLERROR
	ERR_NOT_LEADER
	ERR_RAFT
	ERR_SEND_FAILED
	ERR_RECORD_NOTEXIST
	ERR_MISSING_FIELDS //缺少字段
	ERR_MISSING_TABLE  //没有指定表
	ERR_MISSING_KEY    //没有指定key
	ERR_INVAILD_TABLE  //非法表
	ERR_INVAILD_FIELD  //非法字段
	ERR_CAS_NOT_EQUAL
	ERR_CONNECTION
	ERR_OTHER
	ERR_END
)

/*
	ERR_VERSION        //版本号不匹配
	ERR_NOTFOUND       //数据不存在
	ERR_MISSING_FIELDS //缺少字段
	ERR_MISSING_TABLE  //没有指定表
	ERR_MISSING_KEY    //没有指定key
	ERR_INVAILD_TABLE  //非法表
	ERR_INVAILD_FIELD  //非法字段
	ERR_SEND
	ERR_TIMEOUT //超时
	ERR_CLOSE
	ERR_DISCONNECTED //连接断开
	ERR_SQLERROR     //SQL语句执行出错
	ERR_KEY_EXIST    //key已经存在
	ERR_NOT_EQUAL    //指定的field值与要求的不相等
	ERR_SCAN_END
	ERR_SERVER_STOPED
	ERR_BUSY
	ERR_NOT_LEADER
	ERR_PROPOSAL_DROPPED
	ERR_RAFT
	ERR_ERROR*/

var err_str []string = []string{
	/*	"ok",
		"version mismatch",
		"key not found",
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
		"key already exist",
		"not equal",
		"scan finish",
		"server stoped",
		"busy",
		"not leader",
		"proposal dropped",
		"raft error",
		"error",
	*/
}

func GetErrorStr(code int32) string {

	if code >= 0 && code < ERR_END {
		return err_str[code]
	} else {
		return "invaild errcode"
	}
}
