package client

import (
	"fmt"
	"runtime"
)

type MGetCmd struct {
	priority      int
	callbackQueue EventQueueI
	cmds          []*GetCmd
}

func MGet(cmds ...*GetCmd) *MGetCmd {
	return &MGetCmd{
		cmds: cmds,
	}
}

func MGetWithEventQueue(priority int, callbackQueue EventQueueI, cmds ...*GetCmd) *MGetCmd {
	return &MGetCmd{
		callbackQueue: callbackQueue,
		cmds:          cmds,
		priority:      priority,
	}
}

func (this *MGetCmd) doCallBack(sync bool, cb func([]*GetResult), rets []*GetResult) {
	if !sync && nil != this.callbackQueue {
		this.callbackQueue.Post(this.priority, cb, rets)
	} else {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 65535)
				l := runtime.Stack(buf, false)
				GetSugar().Errorf(formatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
			}
		}()
		cb(rets)
	}
}

func (this *MGetCmd) Exec() []*GetResult {
	respChan := make(chan []*GetResult)
	this.asyncExec(true, func(r []*GetResult) {
		respChan <- r
	})
	return <-respChan
}

func (this *MGetCmd) AsyncExec(cb func([]*GetResult)) {
	this.asyncExec(false, cb)
}

func (this *MGetCmd) asyncExec(sync bool, cb func([]*GetResult)) {
	respCount := 0
	wantCount := len(this.cmds)
	die := make(chan struct{})
	retChan := make(chan func() bool, wantCount)
	results := make([]*GetResult, wantCount)

	for _, v := range this.cmds {
		v.AsyncExec(func(ret *GetResult) {
			select {
			case <-die:
			case retChan <- func() bool {
				for k, v := range this.cmds {
					if v.table == ret.Table && v.key == ret.Key {
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
