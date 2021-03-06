package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/backend/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/proto"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync/atomic"
	"time"
)

/*
 *  这些预定义的Error类型，可以从其名字推出Desc,因此Desc全部设置为空字符串，以节省网络传输字节数
 */
var (
	Err_version_mismatch errcode.Error = errcode.New(errcode.Errcode_version_mismatch, "")
	Err_record_exist     errcode.Error = errcode.New(errcode.Errcode_record_exist, "")
	Err_record_notexist  errcode.Error = errcode.New(errcode.Errcode_record_notexist, "")
	Err_record_unchange  errcode.Error = errcode.New(errcode.Errcode_record_unchange, "")
	Err_cas_not_equal    errcode.Error = errcode.New(errcode.Errcode_cas_not_equal, "")
	Err_timeout          errcode.Error = errcode.New(errcode.Errcode_timeout, "")
)

var processDelay atomic.Value
var disconnectOnRecvMsg atomic.Value

func splitUniKey(unikey string) (table string, key string) {
	i := -1
	for k, v := range unikey {
		if v == 58 {
			i = k
			break
		}
	}

	if i >= 0 {
		table = unikey[:i]
		key = unikey[i+1:]
	}

	return
}

func SetDisconnectOnRecvMsg() {
	disconnectOnRecvMsg.Store(true)
}

func ClearDisconnectOnRecvMsg() {
	disconnectOnRecvMsg.Store(false)
}

func GetDisconnectOnRecvMsg() bool {
	v := disconnectOnRecvMsg.Load()
	if nil == v {
		return false
	} else {
		return v.(bool)
	}
}

func SetProcessDelay(delay time.Duration) {
	processDelay.Store(delay)
}

func getProcessDelay() time.Duration {
	v := processDelay.Load()
	if nil == v {
		return 0
	} else {
		return v.(time.Duration)
	}
}

type handler func(*net.Socket, *cs.ReqMessage)

type kv struct {
	uniKey  string
	key     string
	table   string
	version int64
	meta    db.TableMeta
	fields  map[string]*proto.Field //字段
}

type sqlMetaMgr map[string]*sql.TableMeta

func (this sqlMetaMgr) GetTableMeta(table string) db.TableMeta {
	v, ok := this[table]
	if ok {
		return v
	} else {
		return nil
	}
}

type Node struct {
	listener *cs.Listener
	handlers map[flyproto.CmdType]handler
	queue    *queue.PriorityQueue
	metaMgr  db.MetaMgr
	store    map[string]*kv
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *Node) Register(cmd flyproto.CmdType, h handler) {
	if _, ok := this.handlers[cmd]; !ok {
		this.handlers[cmd] = h
	}
}

func (this *Node) Dispatch(session *net.Socket, cmd flyproto.CmdType, msg *cs.ReqMessage) {
	if GetDisconnectOnRecvMsg() {
		session.Close(nil, 0)
	} else {
		if nil != msg {
			switch cmd {
			default:
				if handler, ok := this.handlers[cmd]; ok {
					delay := getProcessDelay()
					if delay > 0 {
						time.Sleep(delay)
					}
					this.queue.ForceAppend(0, func() {
						handler(session, msg)
					})
				}
			}
		}
	}
}

func (this *Node) startListener() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	this.listener.Serve(func(session *net.Socket) {
		go func() {
			session.SetRecvTimeout(flyproto.PingTime * 2)
			session.SetSendQueueSize(10000)

			session.SetInBoundProcessor(cs.NewReqInboundProcessor())
			session.SetEncoder(&cs.RespEncoder{})

			session.BeginRecv(func(s *net.Socket, m interface{}) {
				msg := m.(*cs.ReqMessage)
				this.Dispatch(session, msg.Cmd, msg)
			})
		}()
	})

	return nil
}

func (this *Node) Start(service string, def *db.DbDef) error {

	m, err := sql.CreateDbMeta(def)
	if nil != err {
		return err
	}

	this.metaMgr = sqlMetaMgr(m)

	if this.listener, err = cs.NewListener("tcp", service, verifyLogin); nil != err {
		return err
	}

	this.queue = queue.NewPriorityQueue(1)

	go func() {
		for {
			closed, v := this.queue.Pop()
			if closed {
				return
			} else {
				switch v.(type) {
				case func():
					v.(func())()
				}
			}
		}
	}()

	go func() {
		if err := this.startListener(); nil != err {
			fmt.Printf("server.Start() error:%s\n", err.Error())
		}
	}()

	return nil
}

func fillDefaultValue(meta db.TableMeta, fields *map[string]*flyproto.Field) {
	meta.FillDefaultValues(*fields)
}

func checkVersion(v1 *int64, v2 int64) bool {
	if nil == v1 {
		return true
	} else {
		return *v1 == v2
	}
}

func (this *Node) del(session *net.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.DelReq)

	v, ok := this.store[msg.UniKey]

	if !ok {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   Err_record_notexist,
			Data:  &flyproto.DelResp{Version: req.GetVersion()},
		})
	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_version_mismatch,
				Data:  &flyproto.DelResp{Version: req.GetVersion()},
			})
		} else {
			delete(this.store, msg.UniKey)
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data:  &flyproto.DelResp{Version: v.version},
			})
		}
	}
}

func (this *Node) get(session *net.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.GetReq)

	table, _ := splitUniKey(msg.UniKey)

	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "table not define"),
			Data:  &flyproto.GetResp{Version: req.GetVersion()},
		})
	} else {
		v, ok := this.store[msg.UniKey]
		if !ok {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_record_notexist,
				Data:  &flyproto.GetResp{Version: req.GetVersion()},
			})
		} else if nil != req.Version && *req.Version == v.version {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_record_unchange,
				Data:  &flyproto.GetResp{Version: req.GetVersion()},
			})
		} else {

			fields := []*flyproto.Field{}

			for _, vv := range v.fields {
				fields = append(fields, vv)
			}

			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data:  &flyproto.GetResp{Version: v.version, Fields: fields},
			})
		}
	}
}

func (this *Node) set(session *net.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.SetReq)
	if len(req.GetFields()) == 0 {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.SetResp{Version: req.GetVersion()},
		})
	} else {
		table, key := splitUniKey(msg.UniKey)
		m := this.metaMgr.GetTableMeta(table)
		if nil == m {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetResp{Version: req.GetVersion()},
			})
			return
		} else if nil != m.CheckFields(req.GetFields()...) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetResp{Version: req.GetVersion()},
			})
			return
		} else {

			v, ok := this.store[msg.UniKey]
			if !ok {
				v = &kv{
					uniKey:  msg.UniKey,
					key:     key,
					table:   table,
					version: time.Now().UnixNano(),
					meta:    m,
					fields:  map[string]*flyproto.Field{},
				}

				fillDefaultValue(v.meta, &v.fields)
			}

			if !checkVersion(req.Version, v.version) {
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Err:   Err_version_mismatch,
					Data:  &flyproto.SetResp{Version: v.version},
				})
			} else {
				v.version++

				for _, vv := range req.GetFields() {
					v.fields[vv.GetName()] = vv
				}

				this.store[msg.UniKey] = v
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Data:  &flyproto.SetResp{Version: v.version},
				})

			}
		}
	}
}

func (this *Node) setNx(session *net.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.SetNxReq)

	if len(req.GetFields()) == 0 {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.SetNxResp{Version: req.GetVersion()},
		})
	} else {
		table, key := splitUniKey(msg.UniKey)
		m := this.metaMgr.GetTableMeta(table)
		if nil == m {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetNxResp{Version: req.GetVersion()},
			})
			return
		} else if nil != m.CheckFields(req.GetFields()...) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetNxResp{Version: req.GetVersion()},
			})
			return
		} else {

			v, ok := this.store[msg.UniKey]
			if ok {

				fields := []*flyproto.Field{}

				for _, field := range v.fields {
					fields = append(fields, field)
				}

				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Err:   Err_record_exist,
					Data: &flyproto.SetNxResp{
						Version: v.version,
						Fields:  fields},
				})

			} else {

				v = &kv{
					uniKey:  msg.UniKey,
					key:     key,
					table:   table,
					version: time.Now().UnixNano(),
					meta:    m,
					fields:  map[string]*flyproto.Field{},
				}

				fillDefaultValue(v.meta, &v.fields)

				v.version++

				for _, vv := range req.GetFields() {
					v.fields[vv.GetName()] = vv
				}

				this.store[msg.UniKey] = v

				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Data: &flyproto.SetNxResp{
						Version: v.version,
					},
				})
			}
		}
	}
}

func (this *Node) compareAndSet(session *net.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.CompareAndSetReq)

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{Version: req.GetVersion()},
		})
		return
	}

	table, _ := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{Version: req.GetVersion()},
		})
		return
	} else if nil != m.CheckFields(req.GetNew(), req.GetOld()) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{Version: req.GetVersion()},
		})
		return
	}

	v, ok := this.store[msg.UniKey]
	if ok {
		if !checkVersion(req.Version, v.version) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_version_mismatch,
				Data:  &flyproto.CompareAndSetResp{Version: v.version},
			})
		} else {
			vv := v.fields[req.GetOld().GetName()]
			if !req.GetOld().IsEqual(vv) {
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Err:   Err_cas_not_equal,
					Data: &flyproto.CompareAndSetResp{Version: v.version,
						Value: vv},
				})
			} else {
				v.fields[req.GetOld().GetName()] = req.GetNew()
				v.version++
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Data:  &flyproto.CompareAndSetResp{Version: v.version},
				})
			}
		}

	} else {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   Err_record_notexist,
			Data:  &flyproto.CompareAndSetResp{Version: req.GetVersion()},
		})
	}
}

func (this *Node) compareAndSetNx(session *net.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.CompareAndSetNxReq)

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{Version: req.GetVersion()},
		})
		return
	}

	table, key := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if m == nil {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{Version: req.GetVersion()},
		})
		return
	} else if nil != m.CheckFields(req.GetNew(), req.GetOld()) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{Version: req.GetVersion()},
		})
		return
	}

	v, ok := this.store[msg.UniKey]

	if !ok {
		v = &kv{
			uniKey:  msg.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*flyproto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		v.fields[req.GetOld().GetName()] = req.GetNew()

		v.version++

		this.store[msg.UniKey] = v

		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Data:  &flyproto.CompareAndSetNxResp{Version: v.version},
		})

	} else {

		if !checkVersion(req.Version, v.version) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_version_mismatch,
				Data:  &flyproto.CompareAndSetNxResp{Version: v.version},
			})
		} else {
			vv := v.fields[req.GetOld().GetName()]
			if !req.GetOld().IsEqual(vv) {
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Err:   Err_cas_not_equal,
					Data: &flyproto.CompareAndSetNxResp{Version: v.version,
						Value: vv},
				})

			} else {
				v.fields[req.GetOld().GetName()] = req.GetNew()
				v.version++
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Data:  &flyproto.CompareAndSetNxResp{Version: v.version},
				})

			}
		}

	}
}

func (this *Node) incrBy(session *net.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.IncrByReq)

	f := req.GetField()

	if nil == f {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{Version: req.GetVersion()},
		})
		return
	}

	table, key := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{Version: req.GetVersion()},
		})
		return
	} else if nil != m.CheckFields(f) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{Version: req.GetVersion()},
		})
		return
	}

	v, ok := this.store[msg.UniKey]

	if !ok {
		v = &kv{
			uniKey:  msg.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*flyproto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		vv := v.fields[f.GetName()]

		v.fields[f.GetName()] = flyproto.PackField(f.GetName(), vv.GetInt()+f.GetInt())

		v.version++

		this.store[msg.UniKey] = v

		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Data: &flyproto.IncrByResp{Version: v.version,
				Field: v.fields[f.GetName()]},
		})

	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_version_mismatch,
				Data:  &flyproto.IncrByResp{Version: v.version},
			})
		} else {

			vv := v.fields[f.GetName()]

			v.fields[f.GetName()] = flyproto.PackField(f.GetName(), vv.GetInt()+f.GetInt())

			v.version++

			this.store[msg.UniKey] = v

			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data: &flyproto.IncrByResp{Version: v.version,
					Field: v.fields[f.GetName()]},
			})

		}
	}
}

func (this *Node) decrBy(session *net.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.DecrByReq)

	f := req.GetField()

	if nil == f {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.DecrByResp{Version: req.GetVersion()},
		})
		return
	}

	table, key := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.DecrByResp{Version: req.GetVersion()},
		})
		return
	} else if nil != m.CheckFields(f) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.DecrByResp{Version: req.GetVersion()},
		})
		return
	}

	v, ok := this.store[msg.UniKey]

	if !ok {
		v = &kv{
			uniKey:  msg.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*flyproto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		vv := v.fields[f.GetName()]

		v.fields[f.GetName()] = flyproto.PackField(f.GetName(), vv.GetInt()-f.GetInt())

		v.version++

		this.store[msg.UniKey] = v

		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Data: &flyproto.DecrByResp{Version: v.version,
				Field: v.fields[f.GetName()]},
		})

	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_version_mismatch,
				Data:  &flyproto.DecrByResp{Version: v.version},
			})
		} else {

			vv := v.fields[f.GetName()]

			v.fields[f.GetName()] = flyproto.PackField(f.GetName(), vv.GetInt()-f.GetInt())

			v.version++

			this.store[msg.UniKey] = v

			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data: &flyproto.DecrByResp{Version: v.version,
					Field: v.fields[f.GetName()]},
			})

		}
	}
}

func (this *Node) kick(session *net.Socket, msg *cs.ReqMessage) {
	session.Send(&cs.RespMessage{
		Seqno: msg.Seqno,
		Data:  &flyproto.KickResp{},
	})
}

func (this *Node) initHandler() *Node {

	this.handlers = map[flyproto.CmdType]handler{}

	this.Register(flyproto.CmdType_Del, this.del)
	this.Register(flyproto.CmdType_Get, this.get)
	this.Register(flyproto.CmdType_Set, this.set)
	this.Register(flyproto.CmdType_SetNx, this.setNx)
	this.Register(flyproto.CmdType_CompareAndSet, this.compareAndSet)
	this.Register(flyproto.CmdType_CompareAndSetNx, this.compareAndSetNx)
	this.Register(flyproto.CmdType_IncrBy, this.incrBy)
	this.Register(flyproto.CmdType_DecrBy, this.decrBy)
	this.Register(flyproto.CmdType_Kick, this.kick)

	return this

}

func New() *Node {
	n := &Node{store: map[string]*kv{}}
	return n.initHandler()
}
