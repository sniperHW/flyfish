package client

type MGetCmd struct {
	priority      int
	callbackQueue EventQueueI
	cmds          []*SliceCmd
}

func MGet(cmds ...*SliceCmd) *MGetCmd {
	return &MGetCmd{
		cmds: cmds,
	}
}

func MGetWithEventQueue(priority int, callbackQueue EventQueueI, cmds ...*SliceCmd) *MGetCmd {
	return &MGetCmd{
		callbackQueue: callbackQueue,
		cmds:          cmds,
		priority:      priority,
	}
}

func (this *MGetCmd) doCallBack(sync bool, cb func([]*SliceResult), rets []*SliceResult) {
	if !sync && nil != this.callbackQueue {
		this.callbackQueue.Post(this.priority, cb, rets)
	} else {
		defer Recover()
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
					if v.req.UniKey == ret.unikey {
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
