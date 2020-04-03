package client

import (
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
)

var ClientTimeout uint32 = 6000 //6sec

var seqno int64

type Client struct {
	conn          *Conn
	closed        int32
	callbackQueue *event.EventQueue //响应回调的事件队列
	compress      bool
}

func (this *Client) pcall(unikey string, cb callback, a interface{}) {
	defer util.Recover(logger)
	switch a.(type) {
	case int32:
		cb.onError(unikey, a.(int32))
	default:
		cb.onResult(unikey, a)
	}
}

func (this *Client) doCallBack(unikey string, cb callback, a interface{}) {
	if nil != this.callbackQueue && cb.sync == false {
		this.callbackQueue.Post(func() {
			switch a.(type) {
			case int32:
				cb.onError(unikey, a.(int32))
			default:
				cb.onResult(unikey, a)
			}
		})
	} else {
		this.pcall(unikey, cb, a)
	}
}

func OpenClient(service string, compress bool, callbackQueue ...*event.EventQueue) *Client {

	c := &Client{
		compress: compress,
	}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	c.conn = openConn(c, service)

	return c
}

func (this *Client) Get(table, key string, fields ...string) *SliceCmd {
	return this.conn.Get(table, key, nil, fields...)
}

func (this *Client) GetAll(table, key string) *SliceCmd {
	return this.conn.GetAll(table, key, nil)
}

func (this *Client) GetWithVersion(table, key string, version int64, fields ...string) *SliceCmd {
	return this.conn.Get(table, key, &version, fields...)
}

func (this *Client) GetAllWithVersion(table, key string, version int64) *SliceCmd {
	return this.conn.GetAll(table, key, &version)
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
