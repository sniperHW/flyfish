package goaio

import (
	"sync"
)

type routine struct {
	nnext  *routine
	taskCh chan func()
}

func (r *routine) run(p *taskPool) {
	var ok bool
	for task := range r.taskCh {
		task()
		for {
			ok, task = p.putRoutine(r)
			if !ok {
				return
			} else if nil != task {
				task()
			} else {
				break
			}
		}
	}
}

type taskPool struct {
	sync.Mutex
	die             bool
	routineCount    int
	maxRoutineCount int
	freeRoutines    *routine
	taskQueue       linkList
}

func newTaskPool(maxRoutineCount int) *taskPool {
	if maxRoutineCount <= 0 {
		maxRoutineCount = 8
	}
	return &taskPool{
		maxRoutineCount: maxRoutineCount,
	}
}

func (p *taskPool) putRoutine(r *routine) (bool, func()) {
	p.Lock()
	if p.die {
		p.Unlock()
		return false, nil
	} else {
		v := p.taskQueue.pop()
		if nil != v {
			p.Unlock()
			return true, v.(func())
		} else {
			var head *routine
			if p.freeRoutines == nil {
				head = r
			} else {
				head = p.freeRoutines.nnext
				p.freeRoutines.nnext = r
			}
			r.nnext = head
			p.freeRoutines = r

			p.Unlock()
		}
		return true, nil
	}
}

func (p *taskPool) getRoutine() *routine {
	if p.freeRoutines == nil {
		return nil
	} else {
		r := p.freeRoutines.nnext
		if r == p.freeRoutines {
			p.freeRoutines = nil
		} else {
			p.freeRoutines.nnext = r.nnext
		}

		r.nnext = nil
		return r
	}
}

func (p *taskPool) addTask(task func()) bool {
	p.Lock()
	if p.die {
		p.Unlock()
		return false
	} else if r := p.getRoutine(); nil != r {
		p.Unlock()
		r.taskCh <- task
		return true
	} else if p.routineCount >= p.maxRoutineCount {
		p.taskQueue.push(task)
		p.Unlock()
		return true
	} else {
		p.routineCount++
		r := &routine{taskCh: make(chan func())}
		p.Unlock()
		go r.run(p)
		r.taskCh <- task
		return true
	}
}

func (p *taskPool) close() {
	p.Lock()
	defer p.Unlock()
	if !p.die {
		p.die = true
		for r := p.getRoutine(); nil != r; r = p.getRoutine() {
			close(r.taskCh)
		}
	}
}
