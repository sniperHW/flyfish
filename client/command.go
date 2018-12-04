package client

import (
	message "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet/util"
	"flyfish/codec"
	"github.com/sniperHW/kendynet"
	"time"
	"sync/atomic"
	"flyfish/errcode"
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

func (this *Client) GetAll(table,key string) *Cmd {
	return nil
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

//记录不存在时设置
func (this *Client) SetNx(table,key string,fields map[string]interface{}) *Cmd {
	if len(fields) == 0 {
		return nil
	}

	req := &message.SetnxReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
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

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Client) CompareAndSet(table,key,field string, oldV ,newV interface{}) *Cmd {

	if oldV == nil || newV == nil {
		return nil
	}

	req := &message.CompareAndSetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		New    : message.PackField(field,newV),
		Old    : message.PackField(field,oldV),
	}

	return &Cmd{
		client : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}		 

} 

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Client) CompareAndSetNx(table,key,field string, oldV ,newV interface{}) *Cmd {
	if oldV == nil || newV == nil {
		return nil
	}

	req := &message.CompareAndSetnxReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		New    : message.PackField(field,newV),
		Old    : message.PackField(field,oldV),
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

func (this *Client) IncrBy(table,key,field string,value int64) *Cmd {
	req := &message.IncrbyReq {
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
}

func (this *Client) DecrBy(table,key,field string,value int64) *Cmd {
	req := &message.DecrbyReq {
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
}

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
		c.result.Version = resp.GetVersion()
		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onSetResp but missing waitResp")
	}
}

func (this *Client) onSetNxResp(resp *message.SetnxResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()		
		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onSetNxResp but missing waitResp")
	}
}

func (this *Client) onCompareAndSetResp(resp *message.CompareAndSetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()
		if c.result.ErrCode == errcode.ERR_OK || c.result.ErrCode == errcode.ERR_NOT_EQUAL {
			c.result.Fields = map[string]*Field{}
			c.result.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
		}

		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onCompareAndSetResp but missing waitResp")
	}
}

func (this *Client) onCompareAndSetNxResp(resp *message.CompareAndSetnxResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()
		if c.result.ErrCode == errcode.ERR_OK || c.result.ErrCode == errcode.ERR_NOT_EQUAL {
			c.result.Fields = map[string]*Field{}
			c.result.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
		}

		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onCompareAndSetnxResp but missing waitResp")
	}
}


func (this *Client) onDelResp(resp *message.DelResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		this.doCallBack(c)		
	}
}

func (this *Client) onIncrByResp(resp *message.IncrbyResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()		
		if 0 == c.result.ErrCode {
			c.result.Fields = map[string]*Field{}
			c.result.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
		}
		this.doCallBack(c)		
	}	
}

func (this *Client) onDecrByResp(resp *message.DecrbyResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()		
		if 0 == c.result.ErrCode {
			c.result.Fields = map[string]*Field{}
			c.result.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
		}
		this.doCallBack(c)		
	}	
}

func (this *Client) onMessage(msg *codec.Message) {	
	this.eventQueue.Post(func() {
		name := msg.GetName()
		//fmt.Println(name)
		if name == "*proto.PingResp" {
			return
		} else if name == "*proto.GetResp" {
			this.onGetResp(msg.GetData().(*message.GetResp))
		} else if name == "*proto.SetResp" {
			this.onSetResp(msg.GetData().(*message.SetResp))
		} else if name == "*proto.SetnxResp" {
			this.onSetNxResp(msg.GetData().(*message.SetnxResp))
		} else if name == "*proto.CompareAndSetResp" {
			this.onCompareAndSetResp(msg.GetData().(*message.CompareAndSetResp))
		} else if name == "*proto.CompareAndSetnxResp" {
			this.onCompareAndSetNxResp(msg.GetData().(*message.CompareAndSetnxResp))
		} else if name == "*proto.DelResp" {
			this.onDelResp(msg.GetData().(*message.DelResp))
		} else if name == "*proto.IncrbyResp" { 
			this.onIncrByResp(msg.GetData().(*message.IncrbyResp))
		} else if name == "*proto.DecrbyResp" { 
			this.onDecrByResp(msg.GetData().(*message.DecrbyResp))
		}  else {

		}
	})
}

