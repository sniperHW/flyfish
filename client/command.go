package client

import (
	protocol "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet/util"
	"flyfish/codec"
	"github.com/sniperHW/kendynet"
	"time"
	"sync/atomic"
	"flyfish/errcode"
)

type Field protocol.Field

func (this *Field) IsNil() bool {
	return (*protocol.Field)(this).IsNil()
}


func (this *Field) GetString() string {
	return (*protocol.Field)(this).GetString()
}

func (this *Field) GetUint() uint64 {
	return (*protocol.Field)(this).GetUint()
}

func (this *Field) GetInt() int64 {
	return (*protocol.Field)(this).GetInt()
}

func (this *Field) GetFloat() float64 {
	return (*protocol.Field)(this).GetFloat()
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
	conn    *Conn
	req     proto.Message
	seqno   int64  
}

func (this Cmd) Exec(cb CommandCallBack) {
	context := &cmdContext {
		seqno    : this.seqno,
		callback : cb,
		req      : this.req,
	}
	this.conn.exec(context)
}


func (this *Conn) Get(table,key string,fields ...string) *Cmd {

	if len(fields) == 0 {
		return nil
	}

	req := &protocol.GetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		Fields : fields,
	}
	
	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}	
}

func (this *Conn) GetAll(table,key string) *Cmd {
	req := &protocol.GetAllReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}
	
	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}
}

func (this *Conn) Set(table,key string,fields map[string]interface{},version ...int64) *Cmd {

	if len(fields) == 0 {
		return nil
	}

	req := &protocol.SetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	for k,v := range(fields) {
		req.Fields = append(req.Fields,protocol.PackField(k,v))
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}
}

//记录不存在时设置
func (this *Conn) SetNx(table,key string,fields map[string]interface{}) *Cmd {
	if len(fields) == 0 {
		return nil
	}

	req := &protocol.SetNxReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}

	for k,v := range(fields) {
		req.Fields = append(req.Fields,protocol.PackField(k,v))
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Conn) CompareAndSet(table,key,field string, oldV ,newV interface{}) *Cmd {

	if oldV == nil || newV == nil {
		return nil
	}

	req := &protocol.CompareAndSetReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		New    : protocol.PackField(field,newV),
		Old    : protocol.PackField(field,oldV),
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}		 

} 

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Conn) CompareAndSetNx(table,key,field string, oldV ,newV interface{}) *Cmd {
	if oldV == nil || newV == nil {
		return nil
	}

	req := &protocol.CompareAndSetNxReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		New    : protocol.PackField(field,newV),
		Old    : protocol.PackField(field,oldV),
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}
}

func (this *Conn) Del(table,key string,version ...int64) *Cmd {

	req := &protocol.DelReq{
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}

}

func (this *Conn) IncrBy(table,key,field string,value int64) *Cmd {
	req := &protocol.IncrByReq {
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		Field  : protocol.PackField(field,value),
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}	
}

func (this *Conn) DecrBy(table,key,field string,value int64) *Cmd {
	req := &protocol.DecrByReq {
		Seqno  : proto.Int64(atomic.AddInt64(&this.seqno,1)),
		Table  : proto.String(table),
		Key    : proto.String(key),
		Field  : protocol.PackField(field,value),
	}

	return &Cmd{
		conn   : this,
		req    : req,
		seqno  : req.GetSeqno(),
	}	
}

func (this *Conn) onGetAllResp(resp *protocol.GetAllResp) {
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

func (this *Conn) onGetResp(resp *protocol.GetResp) {
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

func (this *Conn) onSetResp(resp *protocol.SetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()
		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onSetResp but missing waitResp")
	}
}

func (this *Conn) onSetNxResp(resp *protocol.SetNxResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		c.result.Version = resp.GetVersion()		
		this.doCallBack(c)	
	} else {
		kendynet.Debugln("onSetNxResp but missing waitResp")
	}
}

func (this *Conn) onCompareAndSetResp(resp *protocol.CompareAndSetResp) {
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

func (this *Conn) onCompareAndSetNxResp(resp *protocol.CompareAndSetNxResp) {
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


func (this *Conn) onDelResp(resp *protocol.DelResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		c.result.ErrCode = resp.GetErrCode()
		this.doCallBack(c)		
	}
}

func (this *Conn) onIncrByResp(resp *protocol.IncrByResp) {
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

func (this *Conn) onDecrByResp(resp *protocol.DecrByResp) {
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

func (this *Conn) onMessage(msg *codec.Message) {	
	this.eventQueue.Post(func() {
		name := msg.GetName()
		//fmt.Println(name)
		if name == "*proto.PingResp" {
			return
		} else if name == "*proto.GetResp" {
			this.onGetResp(msg.GetData().(*protocol.GetResp))
		} else if name == "*proto.GetAllResp" {
			this.onGetAllResp(msg.GetData().(*protocol.GetAllResp))
		} else if name == "*proto.SetResp" {
			this.onSetResp(msg.GetData().(*protocol.SetResp))
		} else if name == "*proto.SetNxResp" {
			this.onSetNxResp(msg.GetData().(*protocol.SetNxResp))
		} else if name == "*proto.CompareAndSetResp" {
			this.onCompareAndSetResp(msg.GetData().(*protocol.CompareAndSetResp))
		} else if name == "*proto.CompareAndSetNxResp" {
			this.onCompareAndSetNxResp(msg.GetData().(*protocol.CompareAndSetNxResp))
		} else if name == "*proto.DelResp" {
			this.onDelResp(msg.GetData().(*protocol.DelResp))
		} else if name == "*proto.IncrByResp" { 
			this.onIncrByResp(msg.GetData().(*protocol.IncrByResp))
		} else if name == "*proto.DecrByResp" { 
			this.onDecrByResp(msg.GetData().(*protocol.DecrByResp))
		}  else {

		}
	})
}

