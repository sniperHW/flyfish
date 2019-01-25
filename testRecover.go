package flyfish

import (
	"flyfish/proto"
	"fmt"
)

func TestRecover() {

	for i := 0; i < 10; i++ {

		r := &record{
			writeBackFlag: write_back_insert,
			key:           fmt.Sprintf("key:%d", i),
			table:         "testTable",
			fields:        map[string]*proto.Field{},
		}

		r.fields["name"] = proto.PackField("name", fmt.Sprintf("huangwei%d", i))
		r.fields["age"] = proto.PackField("age", 37+i)

		backupRecord(r)

	}
	backupFile.Close()
	Recover()
}
