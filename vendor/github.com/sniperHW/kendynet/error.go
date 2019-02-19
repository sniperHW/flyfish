package kendynet

import (
	"fmt"
	"net"
)

var (
	ErrServerStarted       = fmt.Errorf("Server already started")
	ErrInvaildNewClientCB  = fmt.Errorf("onNewClient == nil")
	ErrBuffMaxSizeExceeded = fmt.Errorf("bytebuffer: Max Buffer Size Exceeded")
	ErrBuffInvaildAgr      = fmt.Errorf("bytebuffer: Invaild Idx or size")
	ErrSocketClose         = fmt.Errorf("socket close")
	ErrSendQueFull         = fmt.Errorf("send queue full")
	ErrSendTimeout         = fmt.Errorf("send timeout")
	ErrStarted             = fmt.Errorf("already started")
	ErrInvaildBuff         = fmt.Errorf("buff is nil")
	ErrNoReceiver          = fmt.Errorf("receiver == nil")
	ErrInvaildObject       = fmt.Errorf("object == nil")
	ErrInvaildEncoder      = fmt.Errorf("encoder == nil")
)

func IsNetTimeout(err error) bool {
	switch err.(type) {
	case net.Error:
		if err.(net.Error).Timeout() {
			return true
		}
		break
	default:
		break
	}
	return false
}
