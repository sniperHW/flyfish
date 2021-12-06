package main

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

//滑动平均
type MovingAverage struct {
	head    int
	window  []int
	total   int
	average int
	wc      int
}

func NewMovingAverage(window int) *MovingAverage {
	return &MovingAverage{
		window: make([]int, window),
	}
}

func (ma *MovingAverage) Add(v int) {
	if ma.wc < len(ma.window) {
		//窗口没有填满
		ma.window[ma.head] = v
		ma.head = (ma.head + 1) % len(ma.window)
		ma.wc++
	} else {
		ma.total -= ma.window[ma.head]
		ma.window[ma.head] = v
		ma.head = (ma.head + 1) % len(ma.window)
	}
	ma.total += v
	ma.average = ma.total / ma.wc
}

func (ma MovingAverage) GetAverage() int {
	return ma.average
}

type messageFlyGateReport struct {
	id            int
	movingAverage int
	realNum       int
}

type messageGetFlyGateList struct {
	respCh chan interface{}
}

type messageGetFlyGateListResp struct {
	gatelist []pdFlygate
}

type messageChangeGate struct {
	id            int
	movingAverage int
	respCh        chan interface{}
}

type messageChangeGateResp struct {
	ok bool
	id int
}

type pdFlygate struct {
	id            int
	movingAverage int
	realNum       int
}

type pd struct {
	flygates     map[int]*pdFlygate
	messageQueue chan interface{}
}

func (p *pd) start() {
	go func() {
		c := time.Tick(time.Second)
		for _ = range c {
			p.messageQueue <- func() {
				tmp := [][]int{}
				for _, v := range p.flygates {
					tmp = append(tmp, []int{v.id, v.movingAverage, v.realNum})
				}

				sort.Slice(tmp, func(i, j int) bool {
					return tmp[i][0] < tmp[j][0]
				})

				fmt.Println(tmp)
			}
		}
	}()

	go p.run()
}

func (p *pd) run() {
	for v := range p.messageQueue {
		switch v.(type) {
		case func():
			v.(func())()
		case *messageFlyGateReport:
			m := v.(*messageFlyGateReport)
			f := p.flygates[m.id]
			if nil == f {
				f = &pdFlygate{
					id: m.id,
				}
				p.flygates[m.id] = f
			}
			f.movingAverage = m.movingAverage
			f.realNum = m.realNum
		case *messageGetFlyGateList:
			resp := &messageGetFlyGateListResp{}
			for _, v := range p.flygates {
				resp.gatelist = append(resp.gatelist, *v)
			}
			v.(*messageGetFlyGateList).respCh <- resp

		case *messageChangeGate:
			m := v.(*messageChangeGate)

			min := int(math.MaxInt32)
			var minGate *pdFlygate

			average := 0
			for _, v := range p.flygates {
				average += v.movingAverage
				if v.movingAverage < min {
					min = v.movingAverage
					minGate = v
				}

			}
			average = average / len(p.flygates)

			var target *pdFlygate

			old := p.flygates[m.id]

			if old.movingAverage-m.movingAverage > average {
				if float64(minGate.movingAverage+m.movingAverage)/float64(average) < 1.05 {
					target = minGate
				}
			}

			if nil != target && target.id != m.id {
				target.movingAverage += m.movingAverage
				old.movingAverage -= m.movingAverage
				m.respCh <- &messageChangeGateResp{ok: true, id: target.id}
			} else {
				//没有符合
				m.respCh <- &messageChangeGateResp{ok: false}
			}
		}
	}
}

type flygate struct {
	id            int
	movingAverage *MovingAverage
	messageQueue  chan interface{}
	messageCount  int
	pd            *pd
}

func (g *flygate) start() {

	go func() {
		c := time.Tick(time.Second)
		for _ = range c {
			g.messageQueue <- func() {
				messageCount := g.messageCount
				g.movingAverage.Add(messageCount)
				g.messageCount = 0
				//上报
				g.pd.messageQueue <- &messageFlyGateReport{
					id:            g.id,
					movingAverage: g.movingAverage.GetAverage(),
					realNum:       messageCount,
				}
			}
		}
	}()

	go g.run()
}

func (g *flygate) run() {
	for v := range g.messageQueue {
		switch v.(type) {
		case func():
			v.(func())()
		case int:
			//客户端消息
			g.messageCount++
		}
	}
}

var flygateMapMtx sync.Mutex
var flygateMap map[int]*flygate = map[int]*flygate{}

type client struct {
	movingAverage *MovingAverage
	messageQueue  chan interface{}
	messageCount  int
	pd            *pd
	g             *flygate
}

func (c *client) start(sendInterval time.Duration) {

	go func() {
		cc := time.Tick(time.Second * 2)
		for _ = range cc {
			c.messageQueue <- func() {
				c.pd.messageQueue <- &messageChangeGate{
					id:            c.g.id,
					movingAverage: c.movingAverage.GetAverage(),
					respCh:        c.messageQueue,
				}
			}
		}
	}()

	go func() {
		cc := time.Tick(time.Second)
		for _ = range cc {
			c.messageQueue <- func() {
				messageCount := c.messageCount
				c.movingAverage.Add(messageCount)
				c.messageCount = 0
			}
		}
	}()

	go func() {
		cc := time.Tick(sendInterval)
		for _ = range cc {
			c.messageQueue <- func() {
				c.messageCount++
				//向flygate发消息
				c.g.messageQueue <- 1
			}
		}
	}()

	go c.run()
}

func (c *client) run() {
	for v := range c.messageQueue {
		switch v.(type) {
		case func():
			v.(func())()
		case *messageChangeGateResp:
			resp := v.(*messageChangeGateResp)
			if resp.ok {
				old := c.g
				flygateMapMtx.Lock()
				c.g = flygateMap[resp.id]
				flygateMapMtx.Unlock()
				fmt.Printf("change gate old:%d,new:%d\n", old.id, c.g.id)
			}
		}
	}
}

func test1() {
	pd := &pd{
		flygates:     map[int]*pdFlygate{},
		messageQueue: make(chan interface{}, 10000),
	}
	pd.start()

	gate1 := &flygate{
		id:            1,
		movingAverage: NewMovingAverage(5),
		messageQueue:  make(chan interface{}, 10000),
		pd:            pd,
	}

	flygateMap[1] = gate1

	gate2 := &flygate{
		id:            2,
		movingAverage: NewMovingAverage(5),
		messageQueue:  make(chan interface{}, 10000),
		pd:            pd,
	}

	flygateMap[2] = gate2

	gate1.start()
	gate2.start()

	for i := 0; i < 10; i++ {
		c := client{
			movingAverage: NewMovingAverage(5),
			messageQueue:  make(chan interface{}, 10000),
			pd:            pd,
			g:             gate1,
		}
		c.start(time.Millisecond * 10)
	}

	time.AfterFunc(time.Second, func() {
		gate3 := &flygate{
			id:            3,
			movingAverage: NewMovingAverage(5),
			messageQueue:  make(chan interface{}, 10000),
			pd:            pd,
		}

		flygateMapMtx.Lock()
		flygateMap[3] = gate3
		flygateMapMtx.Unlock()

		gate3.start()

	})

	time.AfterFunc(time.Second, func() {
		gate4 := &flygate{
			id:            4,
			movingAverage: NewMovingAverage(5),
			messageQueue:  make(chan interface{}, 10000),
			pd:            pd,
		}

		flygateMapMtx.Lock()
		flygateMap[4] = gate4
		flygateMapMtx.Unlock()

		gate4.start()

	})

	time.AfterFunc(time.Second, func() {
		gate5 := &flygate{
			id:            5,
			movingAverage: NewMovingAverage(5),
			messageQueue:  make(chan interface{}, 10000),
			pd:            pd,
		}

		flygateMapMtx.Lock()
		flygateMap[5] = gate5
		flygateMapMtx.Unlock()

		gate5.start()

	})

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c                          //阻塞直至有信号传入
}

func test2() {
	pd := &pd{
		flygates:     map[int]*pdFlygate{},
		messageQueue: make(chan interface{}, 10000),
	}
	pd.start()

	gate1 := &flygate{
		id:            1,
		movingAverage: NewMovingAverage(5),
		messageQueue:  make(chan interface{}, 10000),
		pd:            pd,
	}

	flygateMap[1] = gate1

	gate2 := &flygate{
		id:            2,
		movingAverage: NewMovingAverage(5),
		messageQueue:  make(chan interface{}, 10000),
		pd:            pd,
	}

	flygateMap[2] = gate2

	gate1.start()
	gate2.start()

	for i := 0; i < 5; i++ {
		c := client{
			movingAverage: NewMovingAverage(5),
			messageQueue:  make(chan interface{}, 10000),
			pd:            pd,
			g:             gate1,
		}
		c.start(time.Millisecond * 100)
	}

	{

		for i := 0; i < 2; i++ {
			c := client{
				movingAverage: NewMovingAverage(5),
				messageQueue:  make(chan interface{}, 10000),
				pd:            pd,
				g:             gate1,
			}
			c.start(time.Millisecond * 10)
		}
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT) //监听指定信号
	_ = <-c                          //阻塞直至有信号传入

}

func main() {
	//test1()
	test2()
}
