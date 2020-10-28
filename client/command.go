package client

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	protocol "github.com/sniperHW/flyfish/proto"
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

type cmdContext struct {
	unikey      string
	deadline    time.Time
	isTimeouted bool
	cb          callback
	req         *net.Message
}

func (this *cmdContext) onError(errCode int32) {
	this.cb.onError(this.unikey, errCode)
}

func (this *cmdContext) onResult(r interface{}) {
	this.cb.onResult(this.unikey, r)
}

type StatusCmd struct {
	conn *Conn
	req  *net.Message
}

func (this *StatusCmd) asyncExec(syncFlag bool, cb func(*StatusResult)) {
	context := &cmdContext{
		cb: callback{
			tt:   cb_status,
			cb:   cb,
			sync: syncFlag,
		},
		unikey: this.req.GetHead().UniKey,
		req:    this.req,
	}
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
	req  *net.Message
}

func (this *SliceCmd) asyncExec(syncFlag bool, cb func(*SliceResult)) {
	context := &cmdContext{
		cb: callback{
			tt:   cb_slice,
			cb:   cb,
			sync: syncFlag,
		},
		unikey: this.req.GetHead().UniKey,
		req:    this.req,
	}
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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, &protocol.GetReq{
		Version: version,
		Fields:  fields,
		All:     false,
	})

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) GetAll(table, key string, version *int64) *SliceCmd {

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, &protocol.GetReq{
		Version: version,
		All:     true,
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

	pbdata := &protocol.SetReq{}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
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
		New: protocol.PackField(field, newV),
		Old: protocol.PackField(field, oldV),
	}

	if len(version) > 0 && version[0] > 0 {
		pbdata.Version = proto.Int64(version[0])
	}

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

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

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

	return &SliceCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) Kick(table, key string) *StatusCmd {
	pbdata := &protocol.KickReq{}

	req := net.NewMessage(net.CommonHead{
		Seqno:   atomic.AddInt64(&seqno, 1),
		UniKey:  table + ":" + key,
		Timeout: ClientTimeout,
	}, pbdata)

	return &StatusCmd{
		conn: this,
		req:  req,
	}
}

func (this *Conn) ReloadTableConf() *StatusCmd {
	pbdata := &protocol.ReloadTableConfReq{}

	req := net.NewMessage(net.CommonHead{
		Seqno: atomic.AddInt64(&seqno, 1),
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

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onSetResp(c *cmdContext, errCode int32, resp *protocol.SetResp) {
	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}
	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onSetNxResp(c *cmdContext, errCode int32, resp *protocol.SetNxResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == errcode.ERR_RECORD_EXIST {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()] = (*Field)(v)
		}
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

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

	this.c.doCallBack(c.unikey, c.cb, &ret)

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

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onDelResp(c *cmdContext, errCode int32, resp *protocol.DelResp) {

	ret := StatusResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onIncrByResp(c *cmdContext, errCode int32, resp *protocol.IncrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == errcode.ERR_OK {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onDecrByResp(c *cmdContext, errCode int32, resp *protocol.DecrByResp) {

	ret := SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	if ret.ErrCode == errcode.ERR_OK {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetField().GetName()] = (*Field)(resp.GetField())
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onKickResp(c *cmdContext, errCode int32, resp *protocol.KickResp) {

	ret := StatusResult{
		ErrCode: errCode,
	}

	this.c.doCallBack(c.unikey, c.cb, &ret)

}

func (this *Conn) onReloadTableConfResp(c *cmdContext, errCode int32, resp *protocol.ReloadTableConfResp) {
	ret := StatusResult{
		ErrCode: errCode,
		ErrStr:  resp.Err,
	}
	this.c.doCallBack(c.unikey, c.cb, &ret)
}

func (this *Conn) onMessage(msg *net.Message) {
	this.eventQueue.Post(func() {
		head := msg.GetHead()
		cmd := protocol.CmdType(msg.GetCmd())
		if cmd != protocol.CmdType_Ping {
			ok, ctx := this.timerMgr.CancelByIndex(uint64(head.Seqno))
			if ok {
				c := ctx.(*cmdContext)
				switch cmd {
				case protocol.CmdType_Get:
					this.onGetResp(c, head.ErrCode, msg.GetData().(*protocol.GetResp))
				case protocol.CmdType_Set:
					this.onSetResp(c, head.ErrCode, msg.GetData().(*protocol.SetResp))
				case protocol.CmdType_SetNx:
					this.onSetNxResp(c, head.ErrCode, msg.GetData().(*protocol.SetNxResp))
				case protocol.CmdType_CompareAndSet:
					this.onCompareAndSetResp(c, head.ErrCode, msg.GetData().(*protocol.CompareAndSetResp))
				case protocol.CmdType_CompareAndSetNx:
					this.onCompareAndSetNxResp(c, head.ErrCode, msg.GetData().(*protocol.CompareAndSetNxResp))
				case protocol.CmdType_Del:
					this.onDelResp(c, head.ErrCode, msg.GetData().(*protocol.DelResp))
				case protocol.CmdType_IncrBy:
					this.onIncrByResp(c, head.ErrCode, msg.GetData().(*protocol.IncrByResp))
				case protocol.CmdType_DecrBy:
					this.onDecrByResp(c, head.ErrCode, msg.GetData().(*protocol.DecrByResp))
				case protocol.CmdType_Kick:
					this.onKickResp(c, head.ErrCode, msg.GetData().(*protocol.KickResp))
				case protocol.CmdType_ReloadTableConf:
					this.onReloadTableConfResp(c, head.ErrCode, msg.GetData().(*protocol.ReloadTableConfResp))
				default:
				}
			}
		}
	})
}
