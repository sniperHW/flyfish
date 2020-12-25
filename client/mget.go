package client

import (
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
)

type MGetCmd struct {
	callbackQueue *event.EventQueue
	cmds          []*SliceCmd
}

func MGet(callbackQueue *event.EventQueue, cmds ...*SliceCmd) *MGetCmd {
	return &MGetCmd{
		callbackQueue: callbackQueue,
		cmds:          cmds,
	}
}

func (this *MGetCmd) doCallBack(sync bool, cb func([]*SliceResult), rets []*SliceResult) {
	if !sync && nil != this.callbackQueue {
		this.callbackQueue.Post(cb, rets)
	} else {
		defer util.Recover(logger)
		cb(rets)
	}
}

func (this *MGetCmd) Exec() []*SliceResult {
	respChan := make(chan []*SliceResult)
	this.asyncExec(true, func(r []*SliceResult) {
		respChan <- r
	})
	return <-respChan
}

func (this *MGetCmd) AsyncExec(cb func([]*SliceResult)) {
	this.asyncExec(false, cb)
}

func (this *MGetCmd) asyncExec(sync bool, cb func([]*SliceResult)) {
	respCount := 0
	wantCount := len(this.cmds)
	die := make(chan struct{})
	retChan := make(chan func() bool, wantCount)
	results := make([]*SliceResult, wantCount)

	for _, v := range this.cmds {
		v.AsyncExec(func(ret *SliceResult) {
			select {
			case <-die:
			case retChan <- func() bool {
				for k, v := range this.cmds {
					if v.req.GetHead().UniKey == ret.unikey {
						results[k] = ret
						respCount++
					}
				}
				if respCount == wantCount {
					this.doCallBack(sync, cb, results)
					return true
				} else {
					return false
				}
			}:
			}
		})
	}

	go func() {
		defer close(die)
		for {
			fn := <-retChan
			if fn() {
				return
			}
		}
	}()
}
