package client

import (
	"container/list"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	//"sync"
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

func UnmarshalJsonField(field *Field, obj interface{}) error {
	if field == nil {
		return nil
	} else if (*protocol.Field)(field).IsBlob() {
		return json.Unmarshal(field.GetBlob(), obj)
	} else if (*protocol.Field)(field).IsString() {
		return json.Unmarshal([]byte(field.GetString()), obj)
	} else {
		return nil
	}
}

type cmdContext struct {
	unikey        string
	deadline      time.Time
	deadlineTimer *time.Timer
	cb            callback
	req           *cs.ReqMessage
	cli           *Client
	listElement   *list.Element
	l             *list.List
	waitResp      *map[int64]*cmdContext
	doCallBack    func(*cmdContext, interface{})
	serverConn    *serverConn
}

func (this *cmdContext) onTimeout() {
	this.doCallBack(this, errcode.New(errcode.Errcode_timeout, "timeout"))
}

type StatusCmd struct {
	client *Client
	req    *cs.ReqMessage
}

func (this *StatusCmd) asyncExec(syncFlag bool, cb func(*StatusResult)) {
	this.client.exec(&cmdContext{
		cb: callback{
			tt:   cb_status,
			cb:   cb,
			sync: syncFlag,
		},
		unikey: this.req.UniKey,
		req:    this.req,
		cli:    this.client,
	})
}

func (this *StatusCmd) AsyncExec(cb func(*StatusResult)) {
	this.asyncExec(false, cb)
}

func (this *StatusCmd) Exec() *StatusResult {
	respChan := make(chan *StatusResult)
	this.asyncExec(true, func(r *StatusResult) {
		respChan <- r
	})
	return <-respChan
}

type SliceCmd struct {
	client *Client
	req    *cs.ReqMessage
}

func (this *SliceCmd) asyncExec(syncFlag bool, cb func(*SliceResult)) {
	this.client.exec(&cmdContext{
		cb: callback{
			tt:   cb_slice,
			cb:   cb,
			sync: syncFlag,
		},
		unikey: this.req.UniKey,
		req:    this.req,
		cli:    this.client,
	})
}

func (this *SliceCmd) AsyncExec(cb func(*SliceResult)) {
	this.asyncExec(false, cb)
}

func (this *SliceCmd) Exec() *SliceResult {
	respChan := make(chan *SliceResult)
	this.asyncExec(true, func(r *SliceResult) {
		respChan <- r
	})
	return <-respChan
}

func (this *Client) get(table, key string, version *int64, fields ...string) *SliceCmd {

	if len(fields) == 0 {
		return nil
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data: &protocol.GetReq{
			Version: version,
			Fields:  fields,
			All:     false,
		},
	}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

func (this *Client) getAll(table, key string, version *int64) *SliceCmd {

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data: &protocol.GetReq{
			Version: version,
			All:     true,
		},
	}

	return &SliceCmd{
		client: this,
		req:    req,
	}

}

func (this *Client) Get(table, key string, fields ...string) *SliceCmd {
	return this.get(table, key, nil, fields...)
}

func (this *Client) GetAll(table, key string) *SliceCmd {
	return this.getAll(table, key, nil)
}

func (this *Client) GetWithVersion(table, key string, version int64, fields ...string) *SliceCmd {
	return this.get(table, key, &version, fields...)
}

func (this *Client) GetAllWithVersion(table, key string, version int64) *SliceCmd {
	return this.getAll(table, key, &version)
}

func (this *Client) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {

	if len(fields) == 0 {
		return nil
	}

	pbdata := &protocol.SetReq{}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &StatusCmd{
		client: this,
		req:    req,
	}
}

//如果不存在则设置,否则返回已存在的记录
func (this *Client) SetNx(table, key string, fields map[string]interface{}) *SliceCmd {
	if len(fields) == 0 {
		return nil
	}

	pbdata := &protocol.SetNxReq{}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}) *SliceCmd {

	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetReq{
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}) *SliceCmd {
	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetNxReq{
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

func (this *Client) Del(table, key string) *StatusCmd {

	pbdata := &protocol.DelReq{}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &StatusCmd{
		client: this,
		req:    req,
	}

}

func (this *Client) IncrBy(table, key, field string, value int64) *SliceCmd {
	pbdata := &protocol.IncrByReq{
		Field: protocol.PackField(field, value),
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

func (this *Client) DecrBy(table, key, field string, value int64) *SliceCmd {
	pbdata := &protocol.DecrByReq{
		Field: protocol.PackField(field, value),
	}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &SliceCmd{
		client: this,
		req:    req,
	}
}

func (this *Client) Kick(table, key string) *StatusCmd {
	pbdata := &protocol.KickReq{}

	req := &cs.ReqMessage{
		Seqno:  atomic.AddInt64(&seqno, 1),
		UniKey: table + ":" + key,
		Data:   pbdata}

	return &StatusCmd{
		client: this,
		req:    req,
	}
}

func (this *serverConn) onGetResp(c *cmdContext, errCode errcode.Error, resp *protocol.GetResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()] = (*Field)(v)
		}
	}

	return ret
}

func (this *serverConn) onSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
}

func (this *serverConn) onSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetNxResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if nil != ret.ErrCode && ret.ErrCode.Code == errcode.Errcode_record_exist {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()] = (*Field)(v)
		}
	}

	return ret
}

func (this *serverConn) onCompareAndSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	return ret

}

func (this *serverConn) onCompareAndSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetNxResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	return ret

}

func (this *serverConn) onDelResp(c *cmdContext, errCode errcode.Error, resp *protocol.DelResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
}

func (this *serverConn) onIncrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.IncrByResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	return ret
}

func (this *serverConn) onDecrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.DecrByResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	return ret

}

func (this *serverConn) onKickResp(c *cmdContext, errCode errcode.Error, resp *protocol.KickResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
	}
}

const resendDelay time.Duration = time.Millisecond * 100

func (this *serverConn) onMessage(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {
		var timerStopOK bool
		this.c.mu.Lock()
		ctx, ok := this.waitResp[msg.Seqno]
		if ok {
			timerStopOK = ctx.deadlineTimer.Stop()
			ctx.deadlineTimer = nil
			delete(this.waitResp, msg.Seqno)
			ctx.waitResp = nil
			if this.removed && 0 == len(this.waitResp) {
				this.session.Close(nil, 0)
			}
		}
		this.c.mu.Unlock()
		if ok {
			if timerStopOK && errcode.GetCode(msg.Err) == errcode.Errcode_retry {
				//重试错误且尚未超时
				remain := ctx.deadline.Sub(time.Now())
				if remain > resendDelay+(5*time.Millisecond) {
					//50ms后重发
					time.AfterFunc(resendDelay, func() {
						//GetSugar().Infof("resend %v", ctx.unikey)
						this.c.exec(ctx)
					})
					return
				}
			}

			var ret interface{}

			switch cmd {
			case protocol.CmdType_Get:
				ret = this.onGetResp(ctx, msg.Err, msg.Data.(*protocol.GetResp))
			case protocol.CmdType_Set:
				ret = this.onSetResp(ctx, msg.Err, msg.Data.(*protocol.SetResp))
			case protocol.CmdType_SetNx:
				ret = this.onSetNxResp(ctx, msg.Err, msg.Data.(*protocol.SetNxResp))
			case protocol.CmdType_CompareAndSet:
				ret = this.onCompareAndSetResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetResp))
			case protocol.CmdType_CompareAndSetNx:
				ret = this.onCompareAndSetNxResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetNxResp))
			case protocol.CmdType_Del:
				ret = this.onDelResp(ctx, msg.Err, msg.Data.(*protocol.DelResp))
			case protocol.CmdType_IncrBy:
				ret = this.onIncrByResp(ctx, msg.Err, msg.Data.(*protocol.IncrByResp))
			case protocol.CmdType_DecrBy:
				ret = this.onDecrByResp(ctx, msg.Err, msg.Data.(*protocol.DecrByResp))
			case protocol.CmdType_Kick:
				ret = this.onKickResp(ctx, msg.Err, msg.Data.(*protocol.KickResp))
			default:
				ret = errcode.New(errcode.Errcode_error, "invaild response")
			}

			ctx.doCallBack(ctx, ret)
		}
	}
}
