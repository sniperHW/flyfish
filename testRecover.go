package flyfish

/*
import (
	"fmt"
	"github.com/sniperHW/flyfish/proto"
)

func TestRecover() {

	for i := 0; i < 10; i++ {

		r := &writeBackRecord{
			writeBackFlag: write_back_insert,
			key:           fmt.Sprintf("key:%d", i),
			table:         "blob",
			fields:        map[string]*proto.Field{},
		}

		r.fields["__version__"] = proto.PackField("__version__", 1)
		r.fields["name"] = proto.PackField("name", fmt.Sprintf("huangwei%d", i))
		r.fields["data"] = proto.PackField("data", []byte("data"))

		backupRecord(r)

	}
	backupFile.Close()
	Recover()
}
*/
