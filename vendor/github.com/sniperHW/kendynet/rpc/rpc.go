package rpc

/*
*  注意,传递给RPC模块的所有回调函数可能在底层信道的接收/发送goroutine上执行，
*  为了避免接收/发送goroutine被阻塞，回调函数中不能调用阻塞函数。
*  如需调用阻塞函数，请在回调中启动一个goroutine来执行
 */

const (
	RPC_REQUEST  = 1
	RPC_RESPONSE = 2
)

type RPCMessage interface {
	Type() byte
	GetSeq() uint64
}

type RPCRequest struct {
	Seq      uint64
	Method   string
	Arg      interface{}
	NeedResp bool
}

type RPCResponse struct {
	Seq uint64
	Err error
	Ret interface{}
}

func (this *RPCRequest) Type() byte {
	return RPC_REQUEST
}

func (this *RPCResponse) Type() byte {
	return RPC_RESPONSE
}

func (this *RPCRequest) GetSeq() uint64 {
	return this.Seq
}

func (this *RPCResponse) GetSeq() uint64 {
	return this.Seq
}

type RPCMessageEncoder interface {
	Encode(RPCMessage) (interface{}, error)
}

type RPCMessageDecoder interface {
	Decode(interface{}) (RPCMessage, error)
}

/*
 *  rpc通道，实现了RPCChannel的类型都可用于发送rpc消息
 */

type RPCChannel interface {
	SendRequest(interface{}) error  //发送RPC请求
	SendResponse(interface{}) error //发送RPC回应
	Name() string
	UID() uint64
}
