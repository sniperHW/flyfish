package client

import (
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
	"time"
)

var ClientTimeout time.Duration = 16 * time.Second

var ServerTimeout time.Duration = 16 * time.Second

type Client struct {
	services      []string
	conns         []*Conn
	closed        int32
	mGetQueue     *event.EventQueue
	callbackQueue *event.EventQueue //响应回调的事件队列
}

func stringHash(s string) int {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}

	return int(hash)
}

func (this *Client) pcall(cb callback, a interface{}) {
	defer util.Recover(logger)
	switch a.(type) {
	case int32:
		cb.onError(a.(int32))
		break
	default:
		cb.onResult(a)
		break
	}
}

func (this *Client) doCallBack(cb callback, a interface{}) {
	if nil != this.callbackQueue {
		this.callbackQueue.Post(func() {
			switch a.(type) {
			case int32:
				cb.onError(a.(int32))
				break
			default:
				cb.onResult(a)
				break
			}
		})
	} else {
		this.pcall(cb, a)
	}
}

func OpenClient(services []string, callbackQueue ...*event.EventQueue) *Client {
	if nil == services || len(services) == 0 {
		return nil
	}

	c := &Client{
		services:  []string{},
		conns:     []*Conn{},
		mGetQueue: event.NewEventQueue(),
	}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	for _, v := range services {
		conn := openConn(c, v)
		c.conns = append(c.conns, conn)
		c.services = append(c.services, v)
	}

	c.startMGetQueue()

	return c
}

func (this *Client) startMGetQueue() {
	go func() {
		this.mGetQueue.Run()
	}()
}

func (this *Client) selectConn(key string) *Conn {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn
}

func (this *Client) Get(table, key string, fields ...string) *SliceCmd {
	return this.selectConn(key).Get(table, key, fields...)
}

func (this *Client) GetAll(table, key string) *SliceCmd {
	return this.selectConn(key).GetAll(table, key)
}

func (this *Client) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {
	return this.selectConn(key).Set(table, key, fields, version...)
}

func (this *Client) SetNx(table, key string, fields map[string]interface{}) *StatusCmd {
	return this.selectConn(key).SetNx(table, key, fields)
}

func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}) *SliceCmd {
	return this.selectConn(key).CompareAndSet(table, key, field, oldV, newV)
}

func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}) *SliceCmd {
	return this.selectConn(key).CompareAndSetNx(table, key, field, oldV, newV)
}

func (this *Client) Del(table, key string, version ...int64) *StatusCmd {
	return this.selectConn(key).Del(table, key, version...)
}

func (this *Client) IncrBy(table, key, field string, value int64) *SliceCmd {
	return this.selectConn(key).IncrBy(table, key, field, value)
}

func (this *Client) DecrBy(table, key, field string, value int64) *SliceCmd {
	return this.selectConn(key).DecrBy(table, key, field, value)
}
