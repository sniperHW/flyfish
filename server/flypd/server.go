package flypd

import (
	//"errors"
	//"fmt"
	"github.com/gogo/protobuf/proto"
	//sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	//"net/url"
	"reflect"
)

func (p *pd) registerMsgHandler(msg proto.Message, handler func(*net.UDPAddr, proto.Message)) {
	if nil != msg {
		p.msgHandler[reflect.TypeOf(msg)] = handler
	}
}

func (p *pd) onMsg(from *net.UDPAddr, msg proto.Message) {
	GetSugar().Infof("onMsg %v", msg)
	if h, ok := p.msgHandler[reflect.TypeOf(msg)]; ok {
		h(from, msg)
	}
}

func (p *pd) initMsgHandler() {
}
