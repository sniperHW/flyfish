package flyfish

import (
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"os"
	"strings"
	"time"
)

func TestSnapshot() {
	kv := map[string]*proto.Field{}
	kv["age"] = proto.PackField("age", 11)
	kv["name"] = proto.PackField("name", "sniper")
	kv["phone"] = proto.PackField("phone", strings.Repeat("a", 1024))

	val := strGet()

	beg := time.Now()

	f, err := os.Create("snapshot")

	if nil != err {
		fmt.Println(err)
		return
	}

	for i := 0; i < 50000; i++ {
		val.append("age").appendField(kv["age"])
		val.append("name").appendField(kv["name"])
		val.append("phone").appendField(kv["phone"])
		if i%1000 == 0 {
			f.Write(val.bytes())
			val.reset()
		}
	}

	f.Sync()
	f.Close()

	fmt.Println("use:", time.Now().Sub(beg))

}
