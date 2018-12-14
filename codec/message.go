package codec

import(
	"github.com/golang/protobuf/proto"
	"flyfish/codec/pb"
	protocol "flyfish/proto"
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

	pb.Register(&protocol.PingReq{},1)
	pb.Register(&protocol.PingResp{},2)

	pb.Register(&protocol.SetReq{},3)
	pb.Register(&protocol.SetResp{},4)

	pb.Register(&protocol.GetReq{},5)
	pb.Register(&protocol.GetResp{},6)

	pb.Register(&protocol.GetAllReq{},7)
	pb.Register(&protocol.GetAllResp{},8)


	pb.Register(&protocol.DelReq{},9)
	pb.Register(&protocol.DelResp{},10)

	pb.Register(&protocol.IncrByReq{},11)
	pb.Register(&protocol.IncrByResp{},12)

	pb.Register(&protocol.DecrByReq{},13)
	pb.Register(&protocol.DecrByResp{},14)

	pb.Register(&protocol.SetNxReq{},15)
	pb.Register(&protocol.SetNxResp{},16)	

	pb.Register(&protocol.CompareAndSetReq{},17)
	pb.Register(&protocol.CompareAndSetResp{},18)

	pb.Register(&protocol.CompareAndSetNxReq{},19)
	pb.Register(&protocol.CompareAndSetNxResp{},20)

	pb.Register(&protocol.ScanReq{},21)
	pb.Register(&protocol.ScanResp{},22)


}
