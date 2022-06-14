package client

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const CompressSize = 16 * 1024 //对超过这个大小的blob字段执行压缩

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
	} else {
		v := field.GetValue()
		switch v.(type) {
		case string, []byte:
			var b []byte
			switch v.(type) {
			case []byte:
				b = v.([]byte)
			case string:
				s := v.(string)
				b = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
					Len:  int(len(s)),
					Cap:  int(len(s)),
					Data: (*reflect.StringHeader)(unsafe.Pointer(&s)).Data,
				}))
			}

			if len(b) == 0 {
				return nil
			} else {
				return json.Unmarshal(b, obj)
			}
		default:
			return nil
		}
	}
}

//对大小>=1k的[]byte字段，执行压缩
func packField(key string, v interface{}) *protocol.Field {
	switch v.(type) {
	case []byte:
		b := v.([]byte)
		var bb []byte
		if len(b) >= CompressSize {
			bb, _ = getCompressor().Compress(b)
			size := make([]byte, 4)
			binary.BigEndian.PutUint32(size, uint32(len(bb)+4))
			bb = append(bb, size...)
		} else {
			bb = b
		}
		return protocol.PackField(key, bb)
	default:
		return protocol.PackField(key, v)
	}
}

func unpackField(f *protocol.Field) (*Field, error) {
	var err error
	if nil != f {
		switch f.GetValue().(type) {
		case []byte:
			b := f.GetBlob()
			if ok, size := checkHeader(b); ok {
				if len(b) >= size+4 {
					if size = int(binary.BigEndian.Uint32(b[len(b)-4:])); size == len(b) {
						if b, err = getDecompressor().Decompress(b[:len(b)-4]); nil == err {
							return (*Field)(protocol.PackField(f.Name, b)), err
						}
					} else {
						err = errors.New("flyfish client unpackField:invaild filed1")
					}
				} else {
					err = errors.New("flyfish client unpackField:invaild filed2")
				}

				if nil != err {
					return (*Field)(protocol.PackField(f.Name, []byte{})), err
				}
			}
		}
	}
	return (*Field)(f), err
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
}

/*
 *  对于len > CompressSize的blob字段，compress将消耗大量cpu,为了避免asyncexec大量占用调用
 *  者的cpu,将asynexec交给单独的线程池执行
 */

type asynExecMgr struct {
	queue chan func()
}

func (m *asynExecMgr) exec(fn func()) {
	m.queue <- fn
}

func newAsynExecMgr() *asynExecMgr {
	m := &asynExecMgr{
		queue: make(chan func(), 65535),
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for v := range m.queue {
				v()
			}
		}()
	}

	return m
}

var asynExec *asynExecMgr = newAsynExecMgr()

func (this *cmdContext) onTimeout() {
	this.cli.doCallBack(this, errcode.New(errcode.Errcode_timeout, "timeout"))
}

type StatusCmd struct {
	client  *Client
	unikey  string
	makeReq func() *cs.ReqMessage
}

func (this *StatusCmd) asyncExec(syncFlag bool, cb func(*StatusResult)) {
	asynExec.exec(func() {
		this.client.exec(&cmdContext{
			cb: callback{
				tt:   cb_status,
				cb:   cb,
				sync: syncFlag,
			},
			unikey: this.unikey,
			req:    this.makeReq(),
			cli:    this.client,
		})
	})
}

/*
 * AsyncExec可能在调用函数的上下文中回调cb(排队的请求超限，直接用retry回调cb),
 * 以下情况可能发生死锁
 *
 * Lock()
 * AsyncExec(func(){
 *	 Lock()
 *   Unlock()
 * })
 * Unlock()
 *
 */

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
	client  *Client
	unikey  string
	makeReq func() *cs.ReqMessage
}

func (this *SliceCmd) asyncExec(syncFlag bool, cb func(*SliceResult)) {
	asynExec.exec(func() {
		this.client.exec(&cmdContext{
			cb: callback{
				tt:   cb_slice,
				cb:   cb,
				sync: syncFlag,
			},
			unikey: this.unikey,
			req:    this.makeReq(),
			cli:    this.client,
		})
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

	unikey := table + ":" + key

	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data: &protocol.GetReq{
					Version: version,
					Fields:  fields,
					All:     false,
				},
			}
		},
	}
}

func (this *Client) getAll(table, key string, version *int64) *SliceCmd {
	unikey := table + ":" + key

	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data: &protocol.GetReq{
					Version: version,
					All:     true,
				},
			}
		},
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

	unikey := table + ":" + key

	return &StatusCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			pbdata := &protocol.SetReq{}

			if len(version) > 0 {
				pbdata.Version = proto.Int64(version[0])
			}

			for k, v := range fields {
				pbdata.Fields = append(pbdata.Fields, packField(k, v))
			}

			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data:   pbdata}
		},
	}
}

//如果不存在则设置,否则返回已存在的记录
func (this *Client) SetNx(table, key string, fields map[string]interface{}) *SliceCmd {
	if len(fields) == 0 {
		return nil
	}

	unikey := table + ":" + key

	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			pbdata := &protocol.SetNxReq{}

			for k, v := range fields {
				pbdata.Fields = append(pbdata.Fields, packField(k, v))
			}

			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data:   pbdata}
		},
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}) *SliceCmd {

	if oldV == nil || newV == nil {
		return nil
	}

	unikey := table + ":" + key

	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data: &protocol.CompareAndSetReq{
					New: packField(field, newV),
					Old: packField(field, oldV),
				}}
		},
	}
}

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}) *SliceCmd {
	if oldV == nil || newV == nil {
		return nil
	}

	unikey := table + ":" + key

	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data: &protocol.CompareAndSetNxReq{
					New: packField(field, newV),
					Old: packField(field, oldV),
				}}
		},
	}
}

func (this *Client) Del(table, key string) *StatusCmd {
	unikey := table + ":" + key
	return &StatusCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data:   &protocol.DelReq{}}
		},
	}
}

func (this *Client) IncrBy(table, key, field string, value int64) *SliceCmd {
	unikey := table + ":" + key
	return &SliceCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data: &protocol.IncrByReq{
					Field: protocol.PackField(field, value),
				}}
		},
	}
}

func (this *Client) Kick(table, key string) *StatusCmd {
	unikey := table + ":" + key
	return &StatusCmd{
		client: this,
		unikey: unikey,
		makeReq: func() *cs.ReqMessage {
			return &cs.ReqMessage{
				Seqno:  atomic.AddInt64(&this.seqno, 1),
				UniKey: unikey,
				Data:   &protocol.KickReq{}}
		},
	}
}

func (this *serverConn) onGetResp(c *cmdContext, errCode errcode.Error, resp *protocol.GetResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	var err error

	if ret.ErrCode == nil {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()], err = unpackField(v)
			if nil != err {
				GetSugar().Errorf("onGetResp %s filed:%s unpackField error:%v", c.unikey, v.GetName(), err)
			}
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

	var err error

	if nil != ret.ErrCode && ret.ErrCode.Code == errcode.Errcode_record_exist {
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()], err = unpackField(v)
			if nil != err {
				GetSugar().Errorf("onSetNxResp %s filed:%s unpackField error:%v", c.unikey, v.GetName(), err)
			}
		}
	}

	return ret
}

func (this *serverConn) onCompareAndSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	var err error
	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()], err = unpackField(resp.GetValue())
		if nil != err {
			GetSugar().Errorf("onCompareAndSetResp %s filed:%s unpackField error:%v", c.unikey, resp.GetValue().GetName(), err)
		}
	}

	return ret

}

func (this *serverConn) onCompareAndSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetNxResp) interface{} {

	ret := &SliceResult{
		ErrCode: errCode,
		Version: resp.GetVersion(),
	}

	var err error
	if ret.ErrCode == nil || ret.ErrCode.Code == errcode.Errcode_cas_not_equal {
		ret.Fields = map[string]*Field{}
		ret.Fields[resp.GetValue().GetName()], err = unpackField(resp.GetValue())
		if nil != err {
			GetSugar().Errorf("onCompareAndSetNxResp %s filed:%s unpackField error:%v", c.unikey, resp.GetValue().GetName(), err)
		}

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

func (this *serverConn) onKickResp(c *cmdContext, errCode errcode.Error, resp *protocol.KickResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
	}
}

const resendDelay time.Duration = time.Millisecond * 100

func (this *serverConn) onMessage(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {
		var resend bool
		this.c.mu.Lock()
		ctx, ok := this.waitResp[msg.Seqno]
		if ok {
			delete(this.waitResp, msg.Seqno)
			ctx.waitResp = nil
			if errcode.GetCode(msg.Err) == errcode.Errcode_retry && ctx.deadline.Sub(time.Now()) >= resendDelay+resendDelay/2 {
				resend = true
			} else {
				ctx.deadlineTimer.Stop()
				ctx.deadlineTimer = nil
			}
		}
		this.c.mu.Unlock()
		if ok {
			if resend {
				time.AfterFunc(resendDelay, func() {
					this.c.exec(ctx)
				})
			} else {
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
				case protocol.CmdType_Kick:
					ret = this.onKickResp(ctx, msg.Err, msg.Data.(*protocol.KickResp))
				default:
					ret = errcode.New(errcode.Errcode_error, "invaild response")
				}
				ctx.cli.doCallBack(ctx, ret)
			}
		}
	}
}
