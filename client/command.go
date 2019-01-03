package client

import (
	"flyfish/codec"
	protocol "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet/util"
	//"github.com/sniperHW/kendynet"
	"flyfish/errcode"
	"sync/atomic"
	"time"
	//"fmt"
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

const (
	wait_none   = 0
	wait_send   = 1
	wait_resp   = 2
	wait_remove = 3
)

type cmdContext struct {
	seqno     int64
	deadline  time.Time
	timestamp int64
	status    int
	cb        callback
	req       proto.Message
	heapIdx   uint32
	key       string
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

func (this *cmdContext) onError(errCode int32) {
	this.cb.onError(errCode)
}

func (this *cmdContext) onResult(r interface{}) {
	this.cb.onResult(r)
}

type StatusCmd struct {
	conn  *Conn
	req   proto.Message
	seqno int64
}

func (this *StatusCmd) Exec(cb func(*StatusResult)) {
	context := &cmdContext{
		seqno: this.seqno,
		cb: callback{
			tt: cb_status,
			cb: cb,
		},
		req: this.req,
	}
	this.conn.exec(context)
}

type SliceCmd struct {
	conn  *Conn
	req   proto.Message
	seqno int64
}

func (this *SliceCmd) Exec(cb func(*SliceResult)) {
	context := &cmdContext{
		seqno: this.seqno,
		cb: callback{
			tt: cb_slice,
			cb: cb,
		},
		req: this.req,
	}
	this.conn.exec(context)
}

func (this *Conn) Get(table, key string, fields ...string) *SliceCmd {

	if len(fields) == 0 {
		return nil
	}

	req := &protocol.GetReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Fields:  fields,
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

func (this *Conn) GetAll(table, key string) *SliceCmd {
	req := &protocol.GetAllReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

func (this *Conn) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {

	if len(fields) == 0 {
		return nil
	}

	req := &protocol.SetReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	for k, v := range fields {
		req.Fields = append(req.Fields, protocol.PackField(k, v))
	}

	return &StatusCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

//记录不存在时设置
func (this *Conn) SetNx(table, key string, fields map[string]interface{}) *StatusCmd {
	if len(fields) == 0 {
		return nil
	}

	req := &protocol.SetNxReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	for k, v := range fields {
		req.Fields = append(req.Fields, protocol.PackField(k, v))
	}

	return &StatusCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Conn) CompareAndSet(table, key, field string, oldV, newV interface{}) *SliceCmd {

	if oldV == nil || newV == nil {
		return nil
	}

	req := &protocol.CompareAndSetReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		New:     protocol.PackField(field, newV),
		Old:     protocol.PackField(field, oldV),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}

}

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Conn) CompareAndSetNx(table, key, field string, oldV, newV interface{}) *SliceCmd {
	if oldV == nil || newV == nil {
		return nil
	}

	req := &protocol.CompareAndSetNxReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		New:     protocol.PackField(field, newV),
		Old:     protocol.PackField(field, oldV),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

func (this *Conn) Del(table, key string, version ...int64) *StatusCmd {

	req := &protocol.DelReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	if len(version) > 0 {
		req.Version = proto.Int64(version[0])
	}

	return &StatusCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}

}

func (this *Conn) IncrBy(table, key, field string, value int64) *SliceCmd {
	req := &protocol.IncrByReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Field:   protocol.PackField(field, value),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

func (this *Conn) DecrBy(table, key, field string, value int64) *SliceCmd {
	req := &protocol.DecrByReq{
		Seqno:   proto.Int64(atomic.AddInt64(&this.seqno, 1)),
		Table:   proto.String(table),
		Key:     proto.String(key),
		Field:   protocol.PackField(field, value),
		Timeout: proto.Int64(int64(requestTimeout)),
	}

	return &SliceCmd{
		conn:  this,
		req:   req,
		seqno: req.GetSeqno(),
	}
}

func (this *Conn) onGetAllResp(resp *protocol.GetAllResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if ret.ErrCode == errcode.ERR_OK {
			ret.Fields = map[string]*Field{}
			for _, v := range resp.Fields {
				ret.Fields[v.GetName()] = (*Field)(v)
			}
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onGetResp(resp *protocol.GetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {
		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if ret.ErrCode == errcode.ERR_OK {
			ret.Fields = map[string]*Field{}
			for _, v := range resp.Fields {
				ret.Fields[v.GetName()] = (*Field)(v)
			}
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onSetResp(resp *protocol.SetResp) {
	//fmt.Println("onSetResp")
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := StatusResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onSetNxResp(resp *protocol.SetNxResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := StatusResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onCompareAndSetResp(resp *protocol.CompareAndSetResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_NOT_EQUAL {
			ret.Fields = map[string]*Field{}
			ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onCompareAndSetNxResp(resp *protocol.CompareAndSetNxResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_NOT_EQUAL {
			ret.Fields = map[string]*Field{}
			ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onDelResp(resp *protocol.DelResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := StatusResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onIncrByResp(resp *protocol.IncrByResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if errcode.ERR_OK == ret.ErrCode {
			ret.Fields = map[string]*Field{}
			ret.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onDecrByResp(resp *protocol.DecrByResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {

		ret := SliceResult{
			ErrCode: resp.GetErrCode(),
			Version: resp.GetVersion(),
		}

		if errcode.ERR_OK == ret.ErrCode {
			ret.Fields = map[string]*Field{}
			ret.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
		}

		this.c.doCallBack(c.cb, &ret)
	}
}

func (this *Conn) onMessage(msg *codec.Message) {
	this.eventQueue.Post(func() {
		name := msg.GetName()
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
		} else if name == "*proto.ScanResp" {
			this.onScanResp(msg.GetData().(*protocol.ScanResp))
		} else {

		}
	})
}
