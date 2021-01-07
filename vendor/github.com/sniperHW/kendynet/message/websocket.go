package message

import (
	"fmt"
	"github.com/sniperHW/kendynet"
)

// The message types are defined in RFC 6455, section 11.8.
const (
	// TextMessage denotes a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	WSTextMessage = 1

	// BinaryMessage denotes a binary data message.
	WSBinaryMessage = 2

	// CloseMessage denotes a close control message. The optional message
	// payload contains a numeric code and text. Use the FormatCloseMessage
	// function to format a close message payload.
	WSCloseMessage = 8

	// PingMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	WSPingMessage = 9

	// PongMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	WSPongMessage = 10
)

/*
 *  WSMessage与普通的ByteBuffer Msg的区别在于多了一个messageType字段
 */
type WSMessage struct {
	messageType int
	buff        *kendynet.ByteBuffer
}

func (this *WSMessage) Bytes() []byte {
	return this.buff.Bytes()
}

func (this *WSMessage) PutBytes(idx uint64, value []byte) error {
	return this.buff.PutBytes(idx, value)
}

func (this *WSMessage) GetBytes(idx uint64, size uint64) ([]byte, error) {
	return this.buff.GetBytes(idx, size)
}

func (this *WSMessage) PutString(idx uint64, value string) error {
	return this.buff.PutString(idx, value)
}

func (this *WSMessage) GetString(idx uint64, size uint64) (string, error) {
	return this.buff.GetString(idx, size)
}

func (this *WSMessage) Type() int {
	return this.messageType
}

func NewWSMessage(messageType int, optional ...interface{}) *WSMessage {
	switch messageType {
	case WSTextMessage, WSBinaryMessage, WSCloseMessage, WSPingMessage, WSPongMessage:
	default:
		return nil
	}

	buff := kendynet.NewByteBuffer(optional...)
	if nil == buff {
		fmt.Printf("nil == buff\n")
		return nil
	}
	return &WSMessage{messageType: messageType, buff: buff}
}
