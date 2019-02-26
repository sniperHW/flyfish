/*
 * 接收和解包器，根据应用需求实现接口
 */

package kendynet

type Receiver interface {
	/*
	*  从session接收数据，解包返回一个消息对象
	 */
	ReceiveAndUnpack(sess StreamSession) (interface{}, error)
}
