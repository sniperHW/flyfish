package gopool

import (
	"errors"
	"sync"
)

type routine struct {
	nnext  *routine
	taskCh chan func()
}

func (r *routine) run(p *Pool) {
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

type Mode int

const (
	QueueMode = Mode(0) //队列模式，如果达到goroutine上限且没有空闲goroutine,将任务置入队列
	GoMode    = Mode(1) //如果达到goroutine上限且没有空闲goroutine,开启单独的goroutine执行
)

type Option struct {
	MaxRoutineCount int //最大goroutine数量
	MaxQueueSize    int //最大排队任务数量
	Mode            Mode
}

const maxCacheItemCount = 4096

var gItemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

type listItem struct {
	nnext *listItem
	v     func()
}

type linkList struct {
	tail      *listItem
	count     int
	freeItems *listItem
	freeCount int
}

func (this *linkList) pushItem(l **listItem, item *listItem) {
	var head *listItem
	if *l == nil {
		head = item
	} else {
		head = (*l).nnext
		(*l).nnext = item
	}
	item.nnext = head
	*l = item
}

func (this *linkList) popItem(l **listItem) *listItem {
	if *l == nil {
		return nil
	} else {
		item := (*l).nnext
		if item == (*l) {
			(*l) = nil
		} else {
			(*l).nnext = item.nnext
		}

		item.nnext = nil
		return item
	}
}

func (this *linkList) getPoolItem(v func()) *listItem {
	/*
	 * 本地的cache中获取listItem,如果没有才去sync.Pool取
	 */
	item := this.popItem(&this.freeItems)
	if nil == item {
		item = gItemPool.Get().(*listItem)
	} else {
		this.freeCount--
	}
	item.v = v
	return item
}

func (this *linkList) putPoolItem(item *listItem) {
	item.v = nil
	if this.freeCount < maxCacheItemCount {
		//如果尚未超过本地cache限制，将listItem放回本地cache供下次使用
		this.freeCount++
		this.pushItem(&this.freeItems, item)
	} else {
		gItemPool.Put(item)
	}
}

func (this *linkList) push(v func()) {
	this.pushItem(&this.tail, this.getPoolItem(v))
	this.count++
}

func (this *linkList) pop() func() {
	item := this.popItem(&this.tail)
	if nil == item {
		return nil
	} else {
		this.count--
		v := item.v
		this.putPoolItem(item)
		return v
	}
}

var defaultPool *Pool = New(Option{
	MaxRoutineCount: 1024,
	Mode:            GoMode,
})

type Pool struct {
	sync.Mutex
	die          bool
	routineCount int
	o            Option
	freeRoutines *routine
	taskQueue    linkList
}

func New(o Option) *Pool {
	switch o.Mode {
	case QueueMode, GoMode:
	default:
		return nil
	}

	if o.MaxRoutineCount == 0 {
		o.MaxRoutineCount = 8
	}

	return &Pool{
		o: o,
	}
}

func (p *Pool) putRoutine(r *routine) (bool, func()) {
	p.Lock()
	if p.die {
		p.Unlock()
		return false, nil
	} else {
		v := p.taskQueue.pop()
		if nil != v {
			p.Unlock()
			return true, v
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

func (p *Pool) getRoutine() *routine {
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

func (p *Pool) Go(task func()) (err error) {
	p.Lock()
	if p.die {
		p.Unlock()
		err = errors.New("closed")
	} else if r := p.getRoutine(); nil != r {
		p.Unlock()
		r.taskCh <- task
	} else {
		if p.routineCount == p.o.MaxRoutineCount {
			switch p.o.Mode {
			case GoMode:
				p.Unlock()
				go task()
			case QueueMode:
				if p.o.MaxQueueSize > 0 && p.taskQueue.count >= p.o.MaxQueueSize {
					err = errors.New("exceed MaxQueueSize")
				} else {
					p.taskQueue.push(task)
				}
				p.Unlock()
			}
		} else {
			p.routineCount++
			r := &routine{taskCh: make(chan func())}
			p.Unlock()
			go r.run(p)
			r.taskCh <- task
		}
	}
	return
}

func (p *Pool) Close() {
	p.Lock()
	defer p.Unlock()
	if !p.die {
		p.die = true
		for r := p.getRoutine(); nil != r; r = p.getRoutine() {
			close(r.taskCh)
		}
	}
}

func Go(f func()) error {
	return defaultPool.Go(f)
}
