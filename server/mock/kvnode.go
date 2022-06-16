package mock

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	//sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/golang/protobuf/proto"
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

type handler func(*fnet.Socket, *cs.ReqMessage)

type kv struct {
	uniKey  string
	key     string
	table   string
	version int64
	meta    db.TableMeta
	fields  map[string]*flyproto.Field //字段
}

type Node struct {
	listener    *cs.Listener
	handlers    map[flyproto.CmdType]handler
	queue       *queue.PriorityQueue
	metaMgr     db.DBMeta
	store       map[string]*kv
	consoleConn *fnet.Udp
}

func (this *Node) Register(cmd flyproto.CmdType, h handler) {
	if _, ok := this.handlers[cmd]; !ok {
		this.handlers[cmd] = h
	}
}

func (this *Node) Dispatch(session *fnet.Socket, cmd flyproto.CmdType, msg *cs.ReqMessage) {
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

	this.listener.Serve(func(session *fnet.Socket) {
		go func() {
			session.SetRecvTimeout(flyproto.PingTime * 2)

			session.SetInBoundProcessor(cs.NewReqInboundProcessor())
			session.SetEncoder(&cs.RespEncoder{})

			session.BeginRecv(func(s *fnet.Socket, m interface{}) {
				msg := m.(*cs.ReqMessage)
				this.Dispatch(session, msg.Cmd, msg)
			})
		}()
	})

	return nil
}

func (this *Node) Stop() {
	this.listener.Close()
	this.consoleConn.Close()
}

func (this *Node) Start(leader bool, service string, console string, def *db.DbDef) error {

	m, _ := sql.CreateDbMeta(def)

	this.metaMgr = m

	var err error

	if this.listener, err = cs.NewListener("tcp", service, fnet.OutputBufLimit{}); nil != err {
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

	this.consoleConn, err = fnet.NewUdp(console, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	/*go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := this.consoleConn.ReadFrom(recvbuff)
			if nil != err {
				return
			} else {
				fmt.Println(msg)
				switch msg.(type) {
				case *sproto.QueryLeader:
					fmt.Println(service, leader)
					this.consoleConn.SendTo(from, &sproto.QueryLeaderResp{Yes: leader})
				}
			}
		}

	}()*/

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

func (this *Node) del(session *fnet.Socket, msg *cs.ReqMessage) {
	_, ok := this.store[msg.UniKey]

	if !ok {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   Err_record_notexist,
			Data:  &flyproto.DelResp{},
		})
	} else {
		delete(this.store, msg.UniKey)
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Data:  &flyproto.DelResp{},
		})
	}
}

func (this *Node) get(session *fnet.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.GetReq)

	table, _ := splitUniKey(msg.UniKey)

	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "table not define"),
			Data:  &flyproto.GetResp{},
		})
	} else {
		v, ok := this.store[msg.UniKey]
		if !ok {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_record_notexist,
				Data:  &flyproto.GetResp{},
			})
		} else if nil != req.Version && *req.Version == v.version {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_record_unchange,
				Data:  &flyproto.GetResp{},
			})
		} else {

			fields := []*flyproto.Field{}

			for _, vv := range v.fields {
				fields = append(fields, vv)
			}

			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data:  &flyproto.GetResp{Version: proto.Int64(v.version), Fields: fields},
			})
		}
	}
}

func (this *Node) set(session *fnet.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.SetReq)
	if len(req.GetFields()) == 0 {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.SetResp{},
		})
	} else {
		table, key := splitUniKey(msg.UniKey)
		m := this.metaMgr.GetTableMeta(table)
		if nil == m {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetResp{},
			})
			return
		} else if nil != m.CheckFields(req.GetFields()...) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetResp{},
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
					Data:  &flyproto.SetResp{},
				})
			} else {
				v.version++

				for _, vv := range req.GetFields() {
					v.fields[vv.GetName()] = vv
				}

				this.store[msg.UniKey] = v
				session.Send(&cs.RespMessage{
					Seqno: msg.Seqno,
					Data:  &flyproto.SetResp{},
				})

			}
		}
	}
}

func (this *Node) setNx(session *fnet.Socket, msg *cs.ReqMessage) {
	req := msg.Data.(*flyproto.SetNxReq)

	if len(req.GetFields()) == 0 {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.SetNxResp{},
		})
	} else {
		table, key := splitUniKey(msg.UniKey)
		m := this.metaMgr.GetTableMeta(table)
		if nil == m {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetNxResp{},
			})
			return
		} else if nil != m.CheckFields(req.GetFields()...) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   errcode.New(errcode.Errcode_error, "field not define"),
				Data:  &flyproto.SetNxResp{},
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
						Fields: fields},
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
					Data:  &flyproto.SetNxResp{},
				})
			}
		}
	}
}

func (this *Node) compareAndSet(session *fnet.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.CompareAndSetReq)

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{},
		})
		return
	}

	table, _ := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{},
		})
		return
	} else if nil != m.CheckFields(req.GetNew(), req.GetOld()) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetResp{},
		})
		return
	}

	v, ok := this.store[msg.UniKey]
	if ok {
		vv := v.fields[req.GetOld().GetName()]
		if !req.GetOld().IsEqual(vv) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_cas_not_equal,
				Data: &flyproto.CompareAndSetResp{
					Value: vv},
			})
		} else {
			v.fields[req.GetOld().GetName()] = req.GetNew()
			v.version++
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data:  &flyproto.CompareAndSetResp{},
			})
		}
	} else {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   Err_record_notexist,
			Data:  &flyproto.CompareAndSetResp{},
		})
	}
}

func (this *Node) compareAndSetNx(session *fnet.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.CompareAndSetNxReq)

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{},
		})
		return
	}

	table, key := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if m == nil {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{},
		})
		return
	} else if nil != m.CheckFields(req.GetNew(), req.GetOld()) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.CompareAndSetNxResp{},
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
		})

	} else {
		vv := v.fields[req.GetOld().GetName()]
		if !req.GetOld().IsEqual(vv) {
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Err:   Err_cas_not_equal,
				Data: &flyproto.CompareAndSetNxResp{
					Value: vv},
			})

		} else {
			v.fields[req.GetOld().GetName()] = req.GetNew()
			v.version++
			session.Send(&cs.RespMessage{
				Seqno: msg.Seqno,
				Data:  &flyproto.CompareAndSetNxResp{},
			})

		}
	}
}

func (this *Node) incrBy(session *fnet.Socket, msg *cs.ReqMessage) {

	req := msg.Data.(*flyproto.IncrByReq)

	f := req.GetField()

	if nil == f {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{},
		})
		return
	}

	table, key := splitUniKey(msg.UniKey)
	m := this.metaMgr.GetTableMeta(table)
	if nil == m {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{},
		})
		return
	} else if nil != m.CheckFields(f) {
		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Err:   errcode.New(errcode.Errcode_error, "field not define"),
			Data:  &flyproto.IncrByResp{},
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
			Data: &flyproto.IncrByResp{
				Field: v.fields[f.GetName()]},
		})

	} else {

		vv := v.fields[f.GetName()]

		v.fields[f.GetName()] = flyproto.PackField(f.GetName(), vv.GetInt()+f.GetInt())

		v.version++

		this.store[msg.UniKey] = v

		session.Send(&cs.RespMessage{
			Seqno: msg.Seqno,
			Data: &flyproto.IncrByResp{
				Field: v.fields[f.GetName()]},
		})

	}
}

func (this *Node) kick(session *fnet.Socket, msg *cs.ReqMessage) {
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
	this.Register(flyproto.CmdType_Kick, this.kick)

	return this

}

func NewKvNode() *Node {
	n := &Node{store: map[string]*kv{}}
	return n.initHandler()
}
