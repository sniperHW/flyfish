package sqlnode

import (
	"github.com/sniperHW/flyfish/util/str"
	"sync"
)

var (
	strBuffDefaultSize = 1024 * 10
	strPool            sync.Pool
)

func newStr() *str.Str {
	return str.NewStr(
		make([]byte, strBuffDefaultSize, strBuffDefaultSize),
		0,
	)
}

func getStr() *str.Str {
	return strPool.Get().(*str.Str)
}

func putStr(str *str.Str) {
	strPool.Put(str)
}

func init() {
	strPool.New = func() interface{} {
		return newStr()
	}
}
