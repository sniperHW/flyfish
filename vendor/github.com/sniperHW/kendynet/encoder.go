/*
 * 编码器接口，根据应用需求实现接口
 */

package kendynet

type EnCoder interface {
	/*
	 *  输入一个对象，输出可供session发送的Message对象
	 */
	EnCode(o interface{}) (Message, error)
}
