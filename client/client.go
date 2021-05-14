package client

import (
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"runtime"
	"strings"
)

var ClientTimeout uint32 = 6000 //6sec

var seqno int64

type EventQueueI interface {
	Post(priority int, fn interface{}, args ...interface{}) error
}

func formatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

func Recover() {
	if r := recover(); r != nil {
		buf := make([]byte, 65535)
		l := runtime.Stack(buf, false)
		GetSugar().Errorf(formatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
	}
}

type Client struct {
	conn          *Conn
	callbackQueue EventQueueI //响应回调的事件队列
	priority      int
	/*
	 *  返回unikey所在的store
	 *  对于连接proxy的方式无需提供,store字段由proxy填写
	 */
	unikeyPlacement func(string) int
}

func (this *Client) callcb(unikey string, cb callback, a interface{}) {
	switch a.(type) {
	case errcode.Error:
		cb.onError(unikey, a.(errcode.Error))
	default:
		cb.onResult(unikey, a)
	}
}

func (this *Client) doCallBack(unikey string, cb callback, a interface{}) {
	if nil != this.callbackQueue && cb.sync == false {
		this.callbackQueue.Post(this.priority, this.callcb, unikey, cb, a)
	} else {
		defer Recover()
		this.callcb(unikey, cb, a)
	}
}

func OpenClient(service string, callbackQueue ...EventQueueI) *Client {

	c := &Client{}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	c.conn = openConn(c, service)

	return c
}

func (this *Client) SetUnikeyPlacement(u func(string) int) *Client {
	this.unikeyPlacement = u
	return this
}

func (this *Client) SetPriority(priority int) {
	this.priority = priority
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

func (this *Client) SetNx(table, key string, fields map[string]interface{}) *SliceCmd {
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
