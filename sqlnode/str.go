package sqlnode

import (
	"github.com/sniperHW/flyfish/util/str"
)

//var (
//	strBuffDefaultSize = 1024 * 10
//	strBuffMaxSize     = 1024 * 64
//	strPool            sync.Pool
//)
//
//func newStr() *str.Str {
//	return str.NewStr(
//		make([]byte, strBuffDefaultSize, strBuffDefaultSize),
//		0,
//	)
//}

func getStr() *str.Str {
	//return strPool.Get().(*str.Str)
	return str.Get()

}

func putStr(s *str.Str) {
	str.Put(s)

	//s.Bytes()
	//
	//s.Reset()
	//strPool.Put(s)
}

//func init() {
//	strPool.New = func() interface{} {
//		return newStr()
//	}
//}
