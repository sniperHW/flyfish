/*
面向流的会话接口
*/

package kendynet

import (
	"net"
	"time"
)

const (
	EventTypeMessage = 1
	EventTypeError   = 2
)

type Event struct {
	EventType int16
	Session   StreamSession
	Data      interface{}
}

type StreamSession interface {

	/*
		发送一个对象，使用encoder将对象编码成一个Message调用SendMessage
	*/
	Send(o interface{}) error

	/*
		直接发送Message
	*/
	SendMessage(msg Message) error

	/*
		关闭会话,如果会话中还有待发送的数据且timeout > 0
		将尝试将数据发送完毕后关闭，如果数据未能完成发送则等到timeout秒之后也会被关闭。

		无论何种情况，调用Close之后SendXXX操作都将返回错误
	*/
	Close(reason string, timeout time.Duration)

	/*
		设置关闭回调，当session被关闭时回调
		其中reason参数表明关闭原因由Close函数传入
		需要注意，回调可能在接收或发送goroutine中调用，如回调函数涉及数据竞争，需要自己加锁保护
	*/
	SetCloseCallBack(cb func(StreamSession, string))

	/*
	 *   设置接收解包器,必须在调用Start前设置，Start成功之后的调用将没有任何效果
	 */
	SetReceiver(r Receiver)

	/*
	 *  设置消息序列化器，用于将一个对象序列化成Message对象，
	 *  如果没有设置Send和PostSend将返回错误(只能调用SendMessage,PostSendMessage发送Message)
	 */
	SetEncoder(encoder EnCoder)

	/*
	 *   启动会话处理
	 */
	Start(eventCB func(*Event)) error

	/*
	 *   获取会话的本端地址
	 */
	LocalAddr() net.Addr

	/*
	 *   获取会话的对端地址
	 */
	RemoteAddr() net.Addr

	/*
	 *   设置用户数据
	 */
	SetUserData(ud interface{})

	/*
	 *   获取用户数据
	 */
	GetUserData() interface{}

	GetUnderConn() interface{}

	SetRecvTimeout(time.Duration)

	SetSendTimeout(time.Duration)
}
