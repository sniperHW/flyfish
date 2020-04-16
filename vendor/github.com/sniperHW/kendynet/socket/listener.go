package socket

import (
	"github.com/sniperHW/kendynet"
)

type Listener interface {
	Serve(onNewClient func(kendynet.StreamSession)) error
}
