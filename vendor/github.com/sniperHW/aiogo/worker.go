package aiogo

import (
	//"fmt"
	"sync"
	"time"
)

type worker struct {
	oplist      opList
	listGuard   sync.Locker
	emptyCond   *sync.Cond
	closed      bool
	emptyWaited int
}

func (self *worker) push(op *op) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		gOpPool.put(op)
		return ErrWatcherClosed
	}

	self.oplist.push(op)
	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}

	return nil
}

func (self *worker) get() (bool, *op) {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	for !self.closed && self.oplist.head.nnext == &self.oplist.tail {
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}

	if self.closed {
		return true, nil
	} else {
		head := self.oplist.head.nnext
		self.oplist.tail.pprev.nnext = nil
		self.oplist.head.nnext = &self.oplist.tail
		self.oplist.tail.pprev = &self.oplist.head
		head.pprev = nil
		return false, head
	}
}

func (self *worker) close() {
	self.listGuard.Lock()

	if self.closed {
		self.listGuard.Unlock()
		return
	}

	self.closed = true
	self.listGuard.Unlock()
	self.emptyCond.Broadcast()
}

func (self *worker) run(waitgroup *sync.WaitGroup) {
	timer := newTimer(self, func(op *op) {
		op.remove()
		if nil != op.done {

			var err error
			if op.tt == Read {
				err = ErrRecvTimeout
			} else if op.tt == Write || op.tt == WriteBuffs {
				err = ErrSendTimeout
			} else {
				err = ErrTimeout
			}

			op.done.Post(&CompleteEvent{
				Conn: op.c,
				Type: op.tt,
				buff: op.buff,
				Size: op.size,
				Ud:   op.ud,
				Err:  err,
			})
		}
	})

	defer func() {
		(*waitgroup).Done()
		timer.Close()
	}()

	var nnext *op

	for {
		closed, op := self.get()
		if closed {
			break
		}

		for ; nil != op; op = nnext {
			nnext = op.nnext
			switch op.tt {
			case timeoutNotify:
				timer.tick()
				gOpPool.put(op)
			case userFunc:
				op.ud.(func())()
				gOpPool.put(op)
			case pollEvent:
				op.c.onActive(op.ud.(int))
				gOpPool.put(op)
			case connClosed:
				c := op.c
				for e := c.pendingRead.front(); nil != e; e = c.pendingRead.front() {
					if e.timer != nil {
						e.timer.Remove(e)
					}
					e.remove()
					gOpPool.put(e)
				}
				for e := c.pendingWrite.front(); nil != e; e = c.pendingWrite.front() {
					if e.timer != nil {
						e.timer.Remove(e)
					}
					e.remove()
					gOpPool.put(e)
				}
				gOpPool.put(op)
			case Read:
				recvTimeout := op.c.getRecvTimeout()
				if recvTimeout != 0 {
					timer.Add(op, time.Now().Add(recvTimeout))
				}
				op.c.pendingRead.push(op)
				op.c.doRead()
			case Write, WriteBuffs:
				sendTimeout := op.c.getSendTimeout()
				if sendTimeout != 0 {
					timer.Add(op, time.Now().Add(sendTimeout))
				}
				op.c.pendingWrite.push(op)
				op.c.doWrite()
			default:
				panic("error tt")
			}
		}

	}
}

func newWorker() *worker {
	self := &worker{}
	self.listGuard = &spinLock{}
	self.emptyCond = sync.NewCond(self.listGuard)
	self.oplist.head.nnext = &self.oplist.tail
	self.oplist.tail.pprev = &self.oplist.head
	return self
}
