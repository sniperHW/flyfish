package client

import (
	//"flyfish/codec"
	//"flyfish/errcode"
	//"github.com/sniperHW/kendynet"
	//"github.com/sniperHW/kendynet/util"
	"github.com/sniperHW/kendynet/event"
	//"github.com/sniperHW/kendynet/socket/stream_socket/tcp"
	//protocol "flyfish/proto"
	//"github.com/golang/protobuf/proto"
	//"time"
	//"runtime"
	//"fmt"
	//"sync/atomic"
)

type Client struct {
	services       []string
	conns          []*Conn
	closed         int32
	mGetQueue      *event.EventQueue
	callbackQueue  *event.EventQueue       //响应回调的事件队列
}

func stringHash(s string) (int) {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}

	return int(hash)
}

func OpenClient(services []string,callbackQueue ...*event.EventQueue) *Client {
	if nil == services || len(services) == 0 {
		return nil
	}

	c := &Client{
		services  : []string{},
		conns     : []*Conn{},
		mGetQueue : event.NewEventQueue(), 
	}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	for _,v := range(services) {
		conn := openConn(v,callbackQueue...)
		c.conns = append(c.conns,conn)
		c.services = append(c.services,v)
	}

	c.startMGetQueue()

	return c
}

func (this *Client) startMGetQueue() {
	go func(){
		this.mGetQueue.Run()
	}()	
}


func (this *Client) Get(table,key string,fields ...string) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.Get(table,key,fields...)
}

func (this *Client) Set(table,key string,fields map[string]interface{},version ...int64) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.Set(table,key,fields,version...)
}

func (this *Client) SetNx(table,key string,fields map[string]interface{}) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.SetNx(table,key,fields)	
}

func (this *Client) CompareAndSet(table,key,field string, oldV ,newV interface{}) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.CompareAndSet(table,key,field,oldV,newV)	
}


func (this *Client) CompareAndSetNx(table,key,field string, oldV ,newV interface{}) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.CompareAndSetNx(table,key,field,oldV,newV)	
}

func (this *Client) Del(table,key string,version ...int64) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.Del(table,key,version...)	
}

func (this *Client) IncrBy(table,key,field string,value int64) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.IncrBy(table,key,field,value)
}

func (this *Client) DecrBy(table,key,field string,value int64) *Cmd {
	var conn *Conn
	if len(this.conns) == 1 {
		conn = this.conns[0]
	} else {
		conn = this.conns[stringHash(key)%len(this.conns)]
	}
	return conn.DecrBy(table,key,field,value)
}




