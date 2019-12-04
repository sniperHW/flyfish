package client

import (
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
)

var ClientTimeout uint32 = 6000 //6sec

//var ServerTimeout time.Duration = 3 * time.Second

type Client struct {
	service       string
	conn          *Conn
	closed        int32
	mGetQueue     *event.EventQueue
	callbackQueue *event.EventQueue //响应回调的事件队列
}

/*
func stringHash(s string) int {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}

	return int(hash)
}*/

func (this *Client) pcall(cb callback, a interface{}) {
	defer util.Recover(logger)
	switch a.(type) {
	case int32:
		cb.onError(a.(int32))
	default:
		cb.onResult(a)
	}
}

func (this *Client) doCallBack(cb callback, a interface{}) {
	if nil != this.callbackQueue {
		this.callbackQueue.Post(func() {
			switch a.(type) {
			case int32:
				cb.onError(a.(int32))
			default:
				cb.onResult(a)
			}
		})
	} else {
		this.pcall(cb, a)
	}
}

func OpenClient(service string, callbackQueue ...*event.EventQueue) *Client {

	c := &Client{
		mGetQueue: event.NewEventQueue(),
		service:   service,
	}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	c.conn = openConn(c, service)

	c.startMGetQueue()

	return c
}

func (this *Client) startMGetQueue() {
	go func() {
		this.mGetQueue.Run()
	}()
}

/*
func (this *Client) selectConn(key string) *Conn {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn
}*/

func (this *Client) Get(table, key string, fields ...string) *SliceCmd {
	return this.conn.Get(table, key, fields...)
}

func (this *Client) GetAll(table, key string) *SliceCmd {
	return this.conn.GetAll(table, key)
}

func (this *Client) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {
	return this.conn.Set(table, key, fields, version...)
}

func (this *Client) SetNx(table, key string, fields map[string]interface{}) *StatusCmd {
	return this.conn.SetNx(table, key, fields)
}

func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {
	return this.conn.CompareAndSet(table, key, field, oldV, newV, version...)
}

func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {
	return this.conn.CompareAndSetNx(table, key, field, oldV, newV, version...)
}

func (this *Client) Del(table, key string, version ...int64) *StatusCmd {
	return this.conn.Del(table, key, version...)
}

func (this *Client) IncrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	return this.conn.IncrBy(table, key, field, value, version...)
}

func (this *Client) DecrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	return this.conn.DecrBy(table, key, field, value, version...)
}

func (this *Client) Kick(table, key string) *StatusCmd {
	return this.conn.Kick(table, key)
}

func (this *Client) ReloadTableConf() *StatusCmd {
	return this.conn.ReloadTableConf()
}
