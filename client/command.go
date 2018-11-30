package client

import (
	//"fmt"
	message "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet/util"
	"flyfish/codec"
	"github.com/sniperHW/kendynet"
	"time"
	"sync/atomic"
)

type Field message.Field

func (this *Field) IsNil() bool {
	return (*message.Field)(this).IsNil()
}


func (this *Field) GetString() string {
	return (*message.Field)(this).GetString()
}

func (this *Field) GetUint() uint64 {
	return (*message.Field)(this).GetUint()
}

func (this *Field) GetInt() int64 {
	return (*message.Field)(this).GetInt()
}

func (this *Field) GetFloat() float64 {
	return (*message.Field)(this).GetFloat()
}


type Result struct {
	ErrCode  int32
	Fields   map[string]*Field
	Version  int64
}

const (
	wait_none   = 0
	wait_send   = 1
	wait_resp   = 2
	wait_remove = 3
)

type CommandCallBack func(*Result)

type cmdContext struct {
	seqno       int64
	result      Result
	deadline    time.Time
	timestamp   int64
	status      int 
	callback    CommandCallBack
	req         proto.Message
	heapIdx     uint32
}

func (this *cmdContext) Less(o util.HeapElement) bool {
	return o.(*cmdContext).deadline.After(this.deadline)
}

func (this *cmdContext) GetIndex() uint32 {
	return this.heapIdx
}

func (this *cmdContext) SetIndex(idx uint32) {
	this.heapIdx = idx
}

type Cmd struct {
	client *Client
	req     proto.Message
	seqno   int64  
}

func (this Cmd) Exec(cb CommandCallBack) {
	context := &cmdContext {
		seqno    : this.seqno,
		callback : cb,
		req      : this.req,
	}
	this.client.exec(context)
}


func (this *Client) Get(table,key string,fields ...string) *Cmd {

	if len(fields) == 0 {
		return nil
	}

	req := &message.GetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		Fields : fields,
	}
	
	return &Cmd{
		client : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}	
}

func (this *Client) Set(table,key string,fields map[string]interface{},version ...int64) *Cmd {

	if len(fields) == 0 {
		return nil
	}

	req := &message.SetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	for k,v := range(fields) {
		req.Fields = append(req.Fields,message.PackField(k,v))
	}

	return &Cmd{
		client : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}
}


func (this *Client) Del(table,key string,version ...int64) *Cmd {

	req := &message.DelReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	return &Cmd{
		client : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}

}
/*
func (this *Client) AtomicInc(table,key,field string,value int64) *Cmd {
	req := &message.AtomicIncreaseReq {
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		Field  : message.PackField(field,value),
	}

	return &Cmd{
		client : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}	
}*/

func (this *Client) onGetResp(resp *message.GetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()
		if 0 == c.result.ErrCode {
			c.result.Fields = map[string]*Field{}
			for _,v := range(resp.Fields) {
				c.result.Fields[v.GetName()] = (*Field)(v)
			}
		}
		this.doCallBack(c)
	}
}

func (this *Client) onSetResp(resp *message.SetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onSetResp but missing waitResp")
	}
}

func (this *Client) onDelResp(resp *message.DelResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		this.doCallBack(c)		
	}
}

/*
func (this *Client) onAtomicIncResp(resp *message.AtomicIncreaseResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		if 0 == c.result.ErrCode {
			c.result.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
		}
		this.doCallBack(c)		
	}	
}*/

func (this *Client) onMessage(msg *codec.Message) {	
	this.eventQueue.Post(func() {
		name := msg.GetName()
		if name == "*proto.PingResp" {
			return
		} else if name == "*proto.GetResp" {
			this.onGetResp(msg.GetData().(*message.GetResp))
		} else if name == "*proto.SetResp" {
			this.onSetResp(msg.GetData().(*message.SetResp))
		} else if name == "*proto.DelResp" {
			this.onDelResp(msg.GetData().(*message.DelResp))
		} /*else if name == "*proto.AtomicIncreaseResp" { 
			this.onAtomicIncResp(msg.GetData().(*message.AtomicIncreaseResp))
		} else {

		}*/
	})
}

