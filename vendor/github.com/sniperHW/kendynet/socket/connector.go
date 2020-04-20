package socket

import (
	"github.com/sniperHW/kendynet"
	"time"
)

type Connector interface {
	Dial(timeout time.Duration) (kendynet.StreamSession, error)
}
