package main 

import(
	message "flyfish/proto"
	"flyfish/codec"
	"fmt"
	"github.com/golang/protobuf/proto"
)

func main() {

	
	req := &message.SetReq{}
	req.Seqno = proto.Int64(100)
	req.Table = proto.String("test")
	req.Key   = proto.String("sniper") 
	encoder := codec.NewEncoder()

	msg,_ := encoder.EnCode(req)

	receiver := codec.NewReceiver()

	pk,_ := receiver.TestUnpack(msg.Bytes())

	fmt.Println(pk.(*codec.Message).GetData())


}