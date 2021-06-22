package gopool

import (
	"errors"
	"sync"
)

type Task interface {
	Do()
}

type routine struct {
	taskCh chan interface{}
}

func (r *routine) run(p *Pool) {
	for task := range r.taskCh {
		switch task.(type) {
		case func():
			task.(func())()
		case Task:
			task.(Task).Do()
		default:
			panic("invaild element")
		}
		p.free(r)
	}
}

var defaultPool *Pool = New(Option{
	MaxRoutineCount: 1024,
	Mode:            GoMode,
})

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

type ring struct {
	head int
	tail int
	data []interface{}
	max  int
}

func newring(max int) ring {
	r := ring{
		max: max,
	}

	var l int
	if max == 0 {
		l = 1024 + 1
	} else {
		l = max + 1
	}

	r.data = make([]interface{}, l, l)

	return r
}

func (r *ring) grow() {
	data := make([]interface{}, len(r.data)*2-1, len(r.data)*2-1)
	i := 0
	for v := r.pop(); nil != v; v = r.pop() {
		data[i] = v
		i++
	}
	r.data = data
	r.head = 0
	r.tail = i
}

func (r *ring) pop() interface{} {
	if r.head != r.tail {
		head := r.data[r.head]
		r.data[r.head] = nil
		r.head = (r.head + 1) % len(r.data)
		return head
	} else {
		return nil
	}
}

func (r *ring) push(v interface{}) bool {
	if (r.tail+1)%len(r.data) != r.head {
		r.data[r.tail] = v
		r.tail = (r.tail + 1) % len(r.data)
		return true
	} else if r.max == 0 {
		r.grow()
		return r.push(v)
	} else {
		return false
	}
}

type Pool struct {
	sync.Mutex
	frees ring
	queue ring
	count int
	o     Option
}

func New(o Option) *Pool {
	switch o.Mode {
	case QueueMode, GoMode:
	default:
		return nil
	}

	if o.MaxRoutineCount == 0 {
		o.MaxRoutineCount = 100
	}

	p := &Pool{
		o:     o,
		frees: newring(o.MaxRoutineCount),
	}

	if o.Mode == QueueMode {
		p.queue = newring(o.MaxQueueSize)
	}

	return p
}

func (p *Pool) free(r *routine) {
	p.Lock()
	defer p.Unlock()
	switch p.o.Mode {
	case QueueMode:
		f := p.queue.pop()
		if nil != f {
			r.taskCh <- f //f.(func())
			return
		}
	}
	p.frees.push(r)
}

func (p *Pool) popFree() *routine {
	if r := p.frees.pop(); nil != r {
		return r.(*routine)
	} else {
		return nil
	}
}

func (p *Pool) gogo(f interface{}) error {
	p.Lock()
	defer p.Unlock()
	r := p.popFree()
	if nil != r {
		r.taskCh <- f
	} else {
		if p.count == p.o.MaxRoutineCount {
			switch p.o.Mode {
			case GoMode:
				switch f.(type) {
				case func():
					go f.(func())()
				case Task:
					go f.(Task).Do()
				default:
					return errors.New("invaild arg")
				}
			case QueueMode:
				if !p.queue.push(f) {
					return errors.New("exceed MaxQueueSize")
				}
			}
		} else {
			p.count++
			r = &routine{
				taskCh: make(chan interface{}, 1),
			}
			r.taskCh <- f
			go r.run(p)
		}
	}
	return nil
}

func (p *Pool) Go(f func()) error {
	return p.gogo(f)
}

func (p *Pool) GoTask(t Task) error {
	return p.gogo(t)
}

func Go(f func()) error {
	return defaultPool.Go(f)
}

func GoTask(t Task) error {
	return defaultPool.GoTask(t)
}
