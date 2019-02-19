package util

import (
	"fmt"
)

type Notifyer struct {
	notiChan chan struct{}
}

func NewNotifyer() *Notifyer {
	return &Notifyer{
		notiChan: make(chan struct{}, 1),
	}
}

func (this *Notifyer) Wait() error {
	_, isClose := <-this.notiChan
	if isClose {
		return fmt.Errorf("closed")
	} else {
		return nil
	}
}

func (this *Notifyer) Notify() {
	select {
	case this.notiChan <- struct{}{}:
	default:
	}
}
