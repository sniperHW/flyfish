package client

import (
	"container/list"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

/*
 *  对于len > CompressSize的blob字段，compress将消耗大量cpu,为了避免asyncexec大量占用调用
 *  者的cpu,将asynexec交给单独的线程池执行
 */
type asynExecMgr struct {
	queue chan func()
	stop  chan struct{}
}

func (m *asynExecMgr) close() {
	close(m.stop)
}

func (m *asynExecMgr) exec(fn func()) {
	select {
	case <-m.stop:
	case m.queue <- fn:
	}
}

func newAsynExecMgr(Ordering bool) *asynExecMgr {
	m := &asynExecMgr{
		queue: make(chan func(), maxPendingSize),
		stop:  make(chan struct{}),
	}

	var worker int
	if Ordering {
		worker = 1
	} else {
		worker = runtime.NumCPU()
	}

	for i := 0; i < worker; i++ {
		go func() {
			for {
				select {
				case <-m.stop:
					return
				case v := <-m.queue:
					v()
				}
			}
		}()
	}
	return m
}

type StatusResult struct {
	ErrCode errcode.Error
	Table   string
	Key     string
	unikey  string
}

type GetResult struct {
	StatusResult
	Version *int64
	Fields  map[string]*Field
}

type ValueResult struct {
	StatusResult
	Value *Field
}

func makeCmdContext(syncCall bool, table string, key string, req proto.Message, deadline time.Time, getErrorResult func(errcode.Error) interface{}, cb interface{}) *cmdContext {
	return &cmdContext{
		key:            key,
		table:          table,
		syncCall:       syncCall,
		cb:             cb,
		deadline:       deadline,
		getErrorResult: getErrorResult,
		slot:           -1,
		req: &cs.ReqMessage{
			UniKey: table + ":" + key,
			Data:   req},
	}
}

type cmd struct {
	client *Client
	key    string
	table  string
	req    proto.Message
}

func (c *cmd) exec(syncCall bool, getErrorResult func(errcode.Error) interface{}, cb interface{}) {
	c.client.exec(makeCmdContext(syncCall, c.table, c.key, c.req, time.Now().Add(ClientTimeout), getErrorResult, cb))
}

type StatusCmd struct {
	cmd
}

func (sc *StatusCmd) getErrorResult(e errcode.Error) interface{} {
	return &StatusResult{
		Key:     sc.key,
		Table:   sc.table,
		ErrCode: e,
	}
}

func (sc *StatusCmd) AsyncExec(cb func(*StatusResult)) {
	sc.exec(false, sc.getErrorResult, cb)
}

func (sc *StatusCmd) Exec() *StatusResult {
	respChan := make(chan *StatusResult)
	sc.exec(true, sc.getErrorResult, func(r *StatusResult) {
		respChan <- r
	})
	return <-respChan
}

type GetCmd struct {
	cmd
}

func (gc *GetCmd) getErrorResult(e errcode.Error) interface{} {
	return &GetResult{
		StatusResult: StatusResult{
			Key:     gc.key,
			Table:   gc.table,
			ErrCode: e,
		},
	}
}

func (gc *GetCmd) AsyncExec(cb func(*GetResult)) {
	gc.exec(false, gc.getErrorResult, cb)
}

func (gc *GetCmd) Exec() *GetResult {
	respChan := make(chan *GetResult)
	gc.exec(true, gc.getErrorResult, func(r *GetResult) {
		respChan <- r
	})
	return <-respChan
}

type ValueCmd struct {
	cmd
}

func (vc *ValueCmd) getErrorResult(e errcode.Error) interface{} {
	return &ValueResult{
		StatusResult: StatusResult{
			Key:     vc.key,
			Table:   vc.table,
			ErrCode: e,
		},
	}
}

func (vc *ValueCmd) AsyncExec(cb func(*ValueResult)) {
	vc.exec(false, vc.getErrorResult, cb)
}

func (vc *ValueCmd) Exec() *ValueResult {
	respChan := make(chan *ValueResult)
	vc.exec(true, vc.getErrorResult, func(r *ValueResult) {
		respChan <- r
	})
	return <-respChan
}

type cmdContext struct {
	key            string
	table          string
	deadline       time.Time
	deadlineTimer  *time.Timer
	req            *cs.ReqMessage
	listElement    *list.Element
	l              *list.List
	emmited        int32
	cb             interface{}
	syncCall       bool
	getErrorResult func(e errcode.Error) interface{}
	slot           int
	store          uint64 //high32:setid,low32:storeid
	leaderVersion  int64
	session        *flynet.Socket
}

func (c *cmdContext) stopTimer() {
	if nil != c.deadlineTimer {
		c.deadlineTimer.Stop()
		c.deadlineTimer = nil
	}
}

func (c *cmdContext) callcb(result interface{}) {
	switch result.(type) {
	case *StatusResult:
		result.(*StatusResult).Key = c.key
		result.(*StatusResult).Table = c.table
	case *GetResult:
		result.(*GetResult).Key = c.key
		result.(*GetResult).Table = c.table
	case *ValueResult:
		result.(*ValueResult).Key = c.key
		result.(*ValueResult).Table = c.table
	default:
		panic("invalid result type")
	}

	switch c.cb.(type) {
	case func(*StatusResult):
		c.cb.(func(*StatusResult))(result.(*StatusResult))
	case func(*GetResult):
		c.cb.(func(*GetResult))(result.(*GetResult))
	case func(*ValueResult):
		c.cb.(func(*ValueResult))(result.(*ValueResult))
	case func(interface{}):
		c.cb.(func(interface{}))(result)
	default:
		panic("invalid cb type")
	}
}

func formatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

func (c *cmdContext) doCallBack(notifyQueue EventQueueI, notifyPriority int, results interface{}, hook func()) {
	if atomic.CompareAndSwapInt32(&c.emmited, 0, 1) {
		if nil != hook {
			hook()
		}
		if notifyQueue == nil || c.syncCall {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 65535)
					l := runtime.Stack(buf, false)
					GetSugar().Errorf(formatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
				}
			}()
			c.callcb(results)
		} else {
			notifyQueue.Post(notifyPriority, c.callcb, results)
		}
	}
}

func (this *Client) get(table, key string, version *int64, fields ...string) *GetCmd {

	if len(fields) == 0 {
		return nil
	} else {
		return &GetCmd{
			cmd: cmd{
				client: this,
				key:    key,
				table:  table,
				req:    &protocol.GetReq{Version: version, Fields: fields, All: false},
			},
		}
	}
}

func (this *Client) getAll(table, key string, version *int64) *GetCmd {
	return &GetCmd{
		cmd: cmd{
			client: this,
			key:    key,
			table:  table,
			req:    &protocol.GetReq{Version: version, All: true},
		},
	}

}

func (this *Client) Get(table, key string, fields ...string) *GetCmd {
	return this.get(table, key, nil, fields...)
}

func (this *Client) GetAll(table, key string) *GetCmd {
	return this.getAll(table, key, nil)
}

func (this *Client) GetWithVersion(table, key string, version int64, fields ...string) *GetCmd {
	return this.get(table, key, &version, fields...)
}

func (this *Client) GetAllWithVersion(table, key string, version int64) *GetCmd {
	return this.getAll(table, key, &version)
}

func (this *Client) Set(table, key string, fields map[string]interface{}, version ...int64) *StatusCmd {

	if len(fields) == 0 {
		return nil
	} else {

		req := &protocol.SetReq{}

		if len(version) > 0 {
			req.Version = proto.Int64(version[0])
		}

		for k, v := range fields {
			req.Fields = append(req.Fields, PackField(k, v))
		}

		return &StatusCmd{
			cmd: cmd{
				client: this,
				key:    key,
				table:  table,
				req:    req,
			},
		}
	}
}

//如果不存在则设置,否则返回已存在的记录
func (this *Client) SetNx(table, key string, fields map[string]interface{}) *ValueCmd {
	if len(fields) == 0 {
		return nil
	} else {
		req := &protocol.SetNxReq{}
		for k, v := range fields {
			req.Fields = append(req.Fields, PackField(k, v))
		}

		return &ValueCmd{
			cmd: cmd{
				client: this,
				key:    key,
				table:  table,
				req:    req,
			},
		}
	}
}

//当记录的field == old时，将其设置为new,并返回field的实际值(如果filed != old,将返回filed的原值)
func (this *Client) CompareAndSet(table, key, field string, oldV, newV interface{}) *ValueCmd {

	if oldV == nil || newV == nil {
		return nil
	} else {
		return &ValueCmd{
			cmd: cmd{
				client: this,
				key:    key,
				table:  table,
				req:    &protocol.CompareAndSetReq{New: PackField(field, newV), Old: PackField(field, oldV)},
			},
		}
	}
}

//当记录不存在或记录的field == old时，将其设置为new.并返回field的实际值(如果记录存在且filed != old,将返回filed的原值)
func (this *Client) CompareAndSetNx(table, key, field string, oldV, newV interface{}) *ValueCmd {
	if oldV == nil || newV == nil {
		return nil
	} else {
		return &ValueCmd{
			cmd: cmd{
				client: this,
				key:    key,
				table:  table,
				req:    &protocol.CompareAndSetNxReq{New: PackField(field, newV), Old: PackField(field, oldV)},
			},
		}
	}
}

func (this *Client) Del(table, key string) *StatusCmd {
	return &StatusCmd{
		cmd: cmd{
			client: this,
			key:    key,
			table:  table,
			req:    &protocol.DelReq{},
		},
	}
}

func (this *Client) IncrBy(table, key, field string, value int64) *ValueCmd {
	return &ValueCmd{
		cmd: cmd{
			client: this,
			key:    key,
			table:  table,
			req:    &protocol.IncrByReq{Field: protocol.PackField(field, value)},
		},
	}
}

func (this *Client) Kick(table, key string) *StatusCmd {
	return &StatusCmd{
		cmd: cmd{
			client: this,
			key:    key,
			table:  table,
			req:    &protocol.KickReq{},
		},
	}
}

func onGetResp(c *cmdContext, errCode errcode.Error, resp *protocol.GetResp) interface{} {

	ret := &GetResult{
		StatusResult: StatusResult{
			ErrCode: errCode,
		},
		Version: resp.Version,
	}

	if ret.ErrCode == nil {
		var err error
		ret.Fields = map[string]*Field{}
		for _, v := range resp.Fields {
			ret.Fields[v.GetName()], err = UnpackField(v)
			if nil != err {
				GetSugar().Errorf("onGetResp %s filed:%s:%s unpackField error:%v", c.table, c.key, v.GetName(), err)
			}
		}
	}

	return ret
}

func onSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
	}
}

func onSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.SetNxResp) interface{} {

	ret := &ValueResult{
		StatusResult: StatusResult{
			ErrCode: errCode,
		},
	}

	if len(resp.Fields) > 0 {
		var err error
		for _, v := range resp.Fields {
			ret.Value, err = UnpackField(v)
			if nil != err {
				GetSugar().Errorf("onSetNxResp %s filed:%s:%s unpackField error:%v", c.table, c.key, v.GetName(), err)
			} else {

			}
			break
		}
	}

	return ret
}

func onCompareAndSetResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetResp) interface{} {

	ret := &ValueResult{
		StatusResult: StatusResult{
			ErrCode: errCode,
		},
	}

	if resp.Value != nil {
		var err error
		ret.Value, err = UnpackField(resp.GetValue())
		if nil != err {
			GetSugar().Errorf("onCompareAndSetResp %s filed:%s:%s unpackField error:%v", c.table, c.key, resp.GetValue().GetName(), err)
		}
	}

	return ret

}

func onCompareAndSetNxResp(c *cmdContext, errCode errcode.Error, resp *protocol.CompareAndSetNxResp) interface{} {

	ret := &ValueResult{
		StatusResult: StatusResult{
			ErrCode: errCode,
		},
	}

	if resp.Value != nil {
		var err error
		ret.Value, err = UnpackField(resp.GetValue())
		if nil != err {
			GetSugar().Errorf("onCompareAndSetNxResp %s filed:%s:%s unpackField error:%v", c.table, c.key, resp.GetValue().GetName(), err)
		}

	}

	return ret

}

func onDelResp(c *cmdContext, errCode errcode.Error, resp *protocol.DelResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
	}
}

func onIncrByResp(c *cmdContext, errCode errcode.Error, resp *protocol.IncrByResp) interface{} {

	ret := &ValueResult{
		StatusResult: StatusResult{
			ErrCode: errCode,
		},
	}

	if resp.Field != nil {
		ret.Value = (*Field)(resp.GetField())
	}

	return ret
}

func onKickResp(c *cmdContext, errCode errcode.Error, resp *protocol.KickResp) interface{} {
	return &StatusResult{
		ErrCode: errCode,
	}
}
