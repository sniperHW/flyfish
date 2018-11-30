package codec

import(
	"github.com/golang/protobuf/proto"
	"flyfish/codec/pb"
	message "flyfish/proto"
)

type Message struct {
	name      string
	data      proto.Message
}

func NewMessage(name string,data proto.Message) *Message {
	return &Message{name:name,data:data}
}

func (this *Message) GetData() proto.Message {
	return this.data
}

func (this *Message) GetName() string {
	return this.name
}

func init() {

	pb.Register(&message.PingReq{},1)
	pb.Register(&message.PingResp{},2)

	pb.Register(&message.SetReq{},3)
	pb.Register(&message.SetResp{},4)

	pb.Register(&message.GetReq{},5)
	pb.Register(&message.GetResp{},6)	

	pb.Register(&message.DelReq{},7)
	pb.Register(&message.DelResp{},8)

	//pb.Register(&message.AtomicIncreaseReq{},9)
	//pb.Register(&message.AtomicIncreaseResp{},10)

}
