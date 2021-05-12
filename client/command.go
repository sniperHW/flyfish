package client

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
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
	unikey        string
	deadline      time.Time
	deadlineTimer *time.Timer
	cb            callback
	req           *cs.ReqMessage
	conn          *Conn
}

func (this *cmdContext) onTimeout() {
	this.conn.Lock()
	_, ok := this.conn.waitResp[this.req.Seqno]
	if ok {
		delete(this.conn.waitResp, this.req.Seqno)
	}
	this.conn.Unlock()

	if ok {
		this.conn.c.doCallBack(this.unikey, this.cb, errcode.New(errcode.Errcode_timeout, "timeout"))
		releaseCmdContext(this)
	}
}

type StatusCmd struct {
	conn *Conn
	req  *cs.ReqMessage
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
	context.conn = this.conn
	this.conn.exec(context)
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
	conn *Conn
	req  *cs.ReqMessage
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
	context.conn = this.conn
	this.conn.exec(context)
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

func (this *Conn) Get(table, key string, version *int64, fields ...string) *SliceCmd {

	if len(fields) == 0 {
		return nil
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data: &protocol.GetReq{
			Version: version,
			Fields:  fields,
			All:     false,
		},
	}

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) GetAll(table, key string, version *int64) *SliceCmd {

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data: &protocol.GetReq{
			Version: version,
			All:     true,
		},
	}

	return &SliceCmd{
		conn: this,
		req:  req,
	}

}

func (this *Conn) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {

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
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

//如果不存在则设置,否则返回已存在的记录
func (this *Conn) SetNx(table, key string, fields map[string]interface{}) *SliceCmd {
	if len(fields) == 0 {
		return nil
	}

	pbdata := &protocol.SetNxReq{}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &SliceCmd{
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
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

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
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) Del(table, key string, version ...int64) *StatusCmd {

	pbdata := &protocol.DelReq{}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &StatusCmd{
		conn: this,
		req:  req,
	}

}

func (this *Conn) IncrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.IncrByReq{
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) DecrBy(table, key, field string, value int64, version ...int64) *SliceCmd {
	pbdata := &protocol.DecrByReq{
		Field: protocol.PackField(field, value),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) Kick(table, key string) *StatusCmd {
	pbdata := &protocol.KickReq{}

	req := &cs.ReqMessage{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
		Data:    pbdata}

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) ReloadTableConf() *StatusCmd {
	pbdata := &protocol.ReloadTableConfReq{}

	req := &cs.ReqMessage{
		Seqno: atomic.AddInt64(&seqno, 1),
		Data:  pbdata}

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) onGetResp(c *cmdContext, errCode errcode.Error, resp *protocol.GetResp) {

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

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetResp) {
	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetNxResp) {

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

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onCompareAndSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onCompareAndSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetNxResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()] = (*Field)(resp.GetValue())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onDelResp(c *cmdContext, errCode errcode.Error, resp *protocol.DelResp) {

	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onIncrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.IncrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onDecrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.DecrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onKickResp(c *cmdContext, errCode errcode.Error, resp *protocol.KickResp) {

	ret := StatusResult{
		ErrCode: errCode,
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onReloadTableConfResp(c *cmdContext, errCode errcode.Error, resp *protocol.ReloadTableConfResp) {
	ret := StatusResult{
		ErrCode: errCode,
		ErrStr:  resp.Err,
	}
	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onMessage(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {
		this.Lock()
		ctx, ok := this.waitResp[msg.Seqno]
		if ok {
			if ok = ctx.deadlineTimer.Stop(); ok {
				delete(this.waitResp, msg.Seqno)
			}
		}
		this.Unlock()
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
			case protocol.CmdType_ReloadTableConf:
				this.onReloadTableConfResp(ctx, msg.Err, msg.Data.(*protocol.ReloadTableConfResp))
			default:
			}
			releaseCmdContext(ctx)
		}
	}
}
