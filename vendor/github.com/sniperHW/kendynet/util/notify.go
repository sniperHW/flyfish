package util

import (
	"errors"
)

var ErrNotifyerClosed error = errors.New("closed")

type Notifyer struct {
	notiChan chan struct{}
}

func NewNotifyer() *Notifyer {
	return &Notifyer{
		notiChan: make(chan struct{}, 1),
	}
}

func (this *Notifyer) Wait() error {
	_, ok := <-this.notiChan
	if !ok {
		return ErrNotifyerClosed
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
