package client

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/util"
	"sync/atomic"
	"time"
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

func (this *Field) GetBlob() []byte {
	return (*protocol.Field)(this).GetBlob()
}

func (this *Field) GetValue() interface{} {
	return (*protocol.Field)(this).GetValue()
}

const (
	wait_none   = 0
	wait_send   = 1
	wait_resp   = 2
	wait_remove = 3
)

type cmdContext struct {
	deadline  time.Time
	timestamp int64
	status    int
	cb        callback
	req       *codec.Message
	heapIdx   uint32
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
	conn *Conn
	req  *codec.Message
}

func (this *StatusCmd) AsyncExec(cb func(*StatusResult)) {
	context := &cmdContext{
		cb: callback{
			tt: cb_status,
			cb: cb,
		},
		req: this.req,
	}
	this.conn.exec(context)
}

func (this *StatusCmd) Exec() *StatusResult {
	respChan := make(chan *StatusResult)
	this.AsyncExec(func(r *StatusResult) {
		respChan <- r
	})
	return <-respChan
}

type SliceCmd struct {
	conn *Conn
	req  *codec.Message
}

func (this *SliceCmd) AsyncExec(cb func(*SliceResult)) {
	context := &cmdContext{
		cb: callback{
			tt: cb_slice,
			cb: cb,
		},
		req: this.req,
	}
	this.conn.exec(context)
}

func (this *SliceCmd) Exec() *SliceResult {
	respChan := make(chan *SliceResult)
	this.AsyncExec(func(r *SliceResult) {
		respChan <- r
	})
	return <-respChan
}

func makeReqCommon(table string, key string /*seqno int64,*/, timeout int64, respTimeout int64) *protocol.ReqCommon {
	return &protocol.ReqCommon{
		Table:       table,
		Key:         key,
		Timeout:     timeout,
		RespTimeout: respTimeout,
	}
}

func (this *Conn) Get(table, key string, fields ...string) *SliceCmd {

	if len(fields) == 0 {
		return nil
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, &protocol.GetReq{
		Head:   makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		Fields: fields,
		All:    false,
	})

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) GetAll(table, key string) *SliceCmd {

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, &protocol.GetReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		All:  true,
	})

	return &SliceCmd{
		conn: this,
		req:  req,
	}

}

func (this *Conn) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {

	if len(fields) == 0 {
		return nil
	}

	pbdata := &protocol.SetReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) SetNx(table, key string, fields map[string]interface{}) *StatusCmd {
	if len(fields) == 0 {
		return nil
	}

	pbdata := &protocol.SetNxReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
	}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Conn) CompareAndSet(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {

	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		New:  protocol.PackField(field, newV),
		Old:  protocol.PackField(field, oldV),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Conn) CompareAndSetNx(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {
	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetNxReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		New:  protocol.PackField(field, newV),
		Old:  protocol.PackField(field, oldV),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) Del(table, key string, version ...int64) *StatusCmd {

	pbdata := &protocol.DelReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}

}

func (this *Conn) IncrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.IncrByReq{
		Head:  makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) DecrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.DecrByReq{
		Head:  makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 {
		pbdata.Head.Version = proto.Int64(version[0])
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) Kick(table, key string) *StatusCmd {
	pbdata := &protocol.KickReq{
		Head: makeReqCommon(table, key, int64(ServerTimeout), int64(ClientTimeout)),
	}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno:  atomic.AddInt64(&this.seqno, 1),
		UniKey: table + ":" + key,
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) ReloadTableConf() *StatusCmd {
	pbdata := &protocol.ReloadTableConfReq{}

	req := codec.NewMessage("", codec.CommonHead{
		Seqno: atomic.AddInt64(&this.seqno, 1),
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) onGetResp(c *cmdContext, errCode int32, resp *protocol.GetResp) {

	ret := SliceResult{
		ErrCode: errCode,
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

func (this *Conn) onSetResp(c *cmdContext, errCode int32, resp *protocol.SetResp) {
	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
	this.c.doCallBack(c.cb, &ret)
}

func (this *Conn) onSetNxResp(c *cmdContext, errCode int32, resp *protocol.SetNxResp) {

	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onCompareAndSetResp(c *cmdContext, errCode int32, resp *protocol.CompareAndSetResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_CAS_NOT_EQUAL {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onCompareAndSetNxResp(c *cmdContext, errCode int32, resp *protocol.CompareAndSetNxResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_CAS_NOT_EQUAL {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onDelResp(c *cmdContext, errCode int32, resp *protocol.DelResp) {

	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onIncrByResp(c *cmdContext, errCode int32, resp *protocol.IncrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if errcode.ERR_OK == ret.ErrCode {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onDecrByResp(c *cmdContext, errCode int32, resp *protocol.DecrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if errcode.ERR_OK == ret.ErrCode {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.NewValue.GetName()] = (*Field)(resp.NewValue)
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onKickResp(c *cmdContext, errCode int32, resp *protocol.KickResp) {

	ret := StatusResult{
		ErrCode: errCode,
	}

	this.c.doCallBack(c.cb, &ret)

}

func (this *Conn) onReloadTableConfResp(c *cmdContext, errCode int32, resp *protocol.ReloadTableConfResp) {
	ret := StatusResult{
		ErrCode: errCode,
		ErrStr:  resp.Err,
	}
	this.c.doCallBack(c.cb, &ret)
}

func (this *Conn) onMessage(msg *codec.Message) {
	this.eventQueue.Post(func() {
		head := msg.GetHead()
		c := this.removeContext(head.Seqno)
		if nil != c {
			name := msg.GetName()
			switch name {
			case "*proto.GetResp":
				this.onGetResp(c, head.ErrCode, msg.GetData().(*protocol.GetResp))
			case "*proto.SetResp":
				this.onSetResp(c, head.ErrCode, msg.GetData().(*protocol.SetResp))
			case "*proto.SetNxResp":
				this.onSetNxResp(c, head.ErrCode, msg.GetData().(*protocol.SetNxResp))
			case "*proto.CompareAndSetResp":
				this.onCompareAndSetResp(c, head.ErrCode, msg.GetData().(*protocol.CompareAndSetResp))
			case "*proto.CompareAndSetNxResp":
				this.onCompareAndSetNxResp(c, head.ErrCode, msg.GetData().(*protocol.CompareAndSetNxResp))
			case "*proto.DelResp":
				this.onDelResp(c, head.ErrCode, msg.GetData().(*protocol.DelResp))
			case "*proto.IncrByResp":
				this.onIncrByResp(c, head.ErrCode, msg.GetData().(*protocol.IncrByResp))
			case "*proto.DecrByResp":
				this.onDecrByResp(c, head.ErrCode, msg.GetData().(*protocol.DecrByResp))
			case "*proto.KickResp":
				this.onKickResp(c, head.ErrCode, msg.GetData().(*protocol.KickResp))
			case "*proto.ReloadTableConfResp":
				this.onReloadTableConfResp(c, head.ErrCode, msg.GetData().(*protocol.ReloadTableConfResp))
			default:
			}
		}
	})

}
