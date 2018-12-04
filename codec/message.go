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

	pb.Register(&message.IncrbyReq{},9)
	pb.Register(&message.IncrbyResp{},10)

	pb.Register(&message.DecrbyReq{},11)
	pb.Register(&message.DecrbyResp{},12)

	pb.Register(&message.SetnxReq{},13)
	pb.Register(&message.SetnxResp{},14)	

	pb.Register(&message.CompareAndSetReq{},15)
	pb.Register(&message.CompareAndSetResp{},16)

	pb.Register(&message.CompareAndSetnxReq{},17)
	pb.Register(&message.CompareAndSetnxResp{},18)

	

}
