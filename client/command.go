package client

import (
	"container/list"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	protocol "github.com/sniperHW/flyfish/proto"
	"sync"
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

var cmdContextPool = sync.Pool{
	New: func() interface{} {
		return &cmdContext{}
	},
}

func getCmdContext() *cmdContext {
	return cmdContextPool.Get().(*cmdContext)
}

func releaseCmdContext(c *cmdContext) {
	cmdContextPool.Put(c)
}

type cmdContext struct {
	mu            *sync.Mutex
	unikey        string
	deadline      time.Time
	deadlineTimer *time.Timer
	cb            callback
	req           *cs.ReqMessage
	serverConn    *serverConn
	listElement   *list.Element
	l             *list.List
	waitResp      *map[int64]*cmdContext
}

func (this *cmdContext) onTimeout() {

	ok := false

	var session *flynet.Socket

	this.mu.Lock()

	if this.waitResp == this.serverConn.waitResp {
		delete(*this.serverConn.waitResp, this.req.Seqno)
		this.waitResp = nil
		ok = true
	}

	if nil != this.listElement {
		this.l.Remove(this.listElement)
		ok = true
	}

	if this.serverConn.removed && 0 == len(*this.serverConn.waitResp) {
		session = this.serverConn.session
	}

	this.mu.Unlock()

	if nil != session {
		session.Close(nil, 0)
	}

	if ok {
		this.serverConn.doCallBack(this.unikey, this.cb, errcode.New(errcode.Errcode_timeout, "timeout"))
		releaseCmdContext(this)
	}

}

type StatusCmd struct {
	client *Client
	req    *cs.ReqMessage
}

func (this *StatusCmd) asyncExec(syncFlag bool, cb func(*StatusResult)) {
	context := getCmdContext()
	context.cb = callback{
		tt:   cb_status,
		cb:   cb,
		sync: syncFlag,
	}
	context.unikey = this.req.UniKey
	context.req = this.req
	context.mu = &this.client.mu
	this.client.exec(context)
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
	context := getCmdContext()
	context.cb = callback{
		tt:   cb_slice,
		cb:   cb,
		sync: syncFlag,
	}
	context.unikey = this.req.UniKey
	context.req = this.req
	context.mu = &this.client.mu
	this.client.exec(context)
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
func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {

	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetReq{
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
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
func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}, version ...int64) *SliceCmd {
	if oldV == nil || newV == nil {
		return nil
	}

	pbdata := &protocol.CompareAndSetNxReq{
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
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

func (this *Client) Del(table, key string, version ...int64) *StatusCmd {

	pbdata := &protocol.DelReq{}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
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

func (this *Client) IncrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.IncrByReq{
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
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

func (this *Client) DecrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.DecrByReq{
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
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

func (this *serverConn) onGetResp(c *cmdContext, errCode errcode.Error, resp *protocol.GetResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()] = (*Field)(v)
		}
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetResp) {
	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
	this.doCallBack(c.unikey, c.cb, &ret)
}

func (this *serverConn) onSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetNxResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if nil != ret.ErrCode && ret.ErrCode.Code == errcode.Errcode_record_exist {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()] = (*Field)(v)
		}
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onCompareAndSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onCompareAndSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetNxResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onDelResp(c *cmdContext, errCode errcode.Error, resp *protocol.DelResp) {

	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onIncrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.IncrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.doCallBack(c.unikey, c.cb, &ret)
}

func (this *serverConn) onDecrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.DecrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onKickResp(c *cmdContext, errCode errcode.Error, resp *protocol.KickResp) {

	ret := StatusResult{
		ErrCode: errCode,
	}

	this.doCallBack(c.unikey, c.cb, &ret)

}

func (this *serverConn) onMessage(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {

		var session *flynet.Socket

		this.mu.Lock()
		ctx, ok := (*this.waitResp)[msg.Seqno]
		if ok {
			if ok = ctx.deadlineTimer.Stop(); ok {
				delete(*this.waitResp, msg.Seqno)
				ctx.waitResp = nil
				if this.removed && 0 == len(*this.waitResp) {
					session = this.session
				}
			}
		}
		this.mu.Unlock()

		if nil != session {
			session.Close(nil, 0)
		}

		if ok {
			switch cmd {
			case protocol.CmdType_Get:
				this.onGetResp(ctx, msg.Err, msg.Data.(*protocol.GetResp))
			case protocol.CmdType_Set:
				this.onSetResp(ctx, msg.Err, msg.Data.(*protocol.SetResp))
			case protocol.CmdType_SetNx:
				this.onSetNxResp(ctx, msg.Err, msg.Data.(*protocol.SetNxResp))
			case protocol.CmdType_CompareAndSet:
				this.onCompareAndSetResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetResp))
			case protocol.CmdType_CompareAndSetNx:
				this.onCompareAndSetNxResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetNxResp))
			case protocol.CmdType_Del:
				this.onDelResp(ctx, msg.Err, msg.Data.(*protocol.DelResp))
			case protocol.CmdType_IncrBy:
				this.onIncrByResp(ctx, msg.Err, msg.Data.(*protocol.IncrByResp))
			case protocol.CmdType_DecrBy:
				this.onDecrByResp(ctx, msg.Err, msg.Data.(*protocol.DecrByResp))
			case protocol.CmdType_Kick:
				this.onKickResp(ctx, msg.Err, msg.Data.(*protocol.KickResp))
			default:
			}
			releaseCmdContext(ctx)
		}
	}
}
