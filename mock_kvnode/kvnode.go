package mock_kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/flyfish/proto"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"time"
)

type handler func(kendynet.StreamSession, *net.Message)

type kv struct {
	uniKey  string
	key     string
	table   string
	version int64
	meta    *dbmeta.TableMeta
	fields  map[string]*proto.Field //字段
}

type Node struct {
	listener *net.Listener
	handlers map[uint16]handler
	queue    *event.EventQueue
	meta     *dbmeta.DBMeta
	store    map[string]*kv
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func (this *Node) Register(cmd uint16, h handler) {
	if _, ok := this.handlers[cmd]; !ok {
		this.handlers[cmd] = h
	}
}

func (this *Node) Dispatch(session kendynet.StreamSession, cmd uint16, msg *net.Message) {
	if nil != msg {
		switch cmd {
		default:
			if handler, ok := this.handlers[cmd]; ok {
				this.queue.PostNoWait(handler, session, msg)
			}
		}
	}
}

func (this *Node) startListener() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession, compress bool) {
		go func() {
			session.SetRecvTimeout(protocol.PingTime * 2)
			session.SetSendQueueSize(10000)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetReceiver(net.NewReceiver(pb.GetNamespace("request"), compress))
			session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))

			session.Start(func(event *kendynet.Event) {
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					msg := event.Data.(*net.Message)
					this.Dispatch(session, msg.GetCmd(), msg)
				}
			})
		}()
	})
}

func (this *Node) Start(service string, meta []string) error {

	var err error

	this.meta, err = dbmeta.NewDBMeta(meta)

	if nil != err {
		return err
	}

	if this.listener, err = net.NewListener("tcp", service, verifyLogin); nil != err {
		return err
	}

	this.queue = event.NewEventQueue()

	go func() {
		this.queue.Run()
	}()

	go func() {
		if err := this.startListener(); nil != err {
			fmt.Printf("server.Start() error:%s\n", err.Error())
		}
	}()

	return nil
}

func fillDefaultValue(meta *dbmeta.TableMeta, fields *map[string]*proto.Field) {
	for name, v := range meta.GetFieldMetas() {
		if _, ok := (*fields)[name]; !ok {
			(*fields)[name] = proto.PackField(name, v.GetDefaultV())
		}
	}
}

func checkVersion(v1 *int64, v2 int64) bool {
	if nil == v1 {
		return true
	} else {
		return *v1 == v2
	}
}

func (this *Node) del(session kendynet.StreamSession, msg *net.Message) {

	req := msg.GetData().(*proto.DelReq)

	head := msg.GetHead()

	v, ok := this.store[head.UniKey]

	if !ok {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_RECORD_EXIST,
		}, &proto.DelResp{Version: req.GetVersion()}))
	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_VERSION_MISMATCH,
			}, &proto.DelResp{Version: v.version}))
		} else {
			delete(this.store, head.UniKey)
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_OK,
			}, &proto.DelResp{Version: v.version}))
		}
	}
}

func (this *Node) get(session kendynet.StreamSession, msg *net.Message) {
	req := msg.GetData().(*proto.GetReq)

	head := msg.GetHead()

	table, _ := head.SplitUniKey()

	m := this.meta.GetTableMeta(table)
	if nil == m {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_TABLE,
		}, &proto.GetResp{Version: req.GetVersion()}))
	} else {
		v, ok := this.store[head.UniKey]
		if !ok {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_RECORD_NOTEXIST,
			}, &proto.GetResp{Version: req.GetVersion()}))
		} else if nil != req.Version && *req.Version == v.version {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_RECORD_UNCHANGE,
			}, &proto.GetResp{Version: req.GetVersion()}))
		} else {

			fields := []*proto.Field{}

			for _, vv := range v.fields {
				fields = append(fields, vv)
			}

			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_OK,
			}, &proto.GetResp{Version: v.version, Fields: fields}))
		}
	}
}

func (this *Node) set(session kendynet.StreamSession, msg *net.Message) {
	req := msg.GetData().(*proto.SetReq)
	head := msg.GetHead()
	if len(req.GetFields()) == 0 {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_MISSING_FIELDS,
		}, &proto.SetResp{
			Version: req.GetVersion(),
		}))
	} else {
		table, key := head.SplitUniKey()
		m := this.meta.GetTableMeta(table)
		if nil == m {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_INVAILD_TABLE,
			}, &proto.SetResp{
				Version: req.GetVersion(),
			}))
			return
		} else if !m.CheckSet(req.GetFields()) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_INVAILD_FIELD,
			}, &proto.SetResp{
				Version: req.GetVersion(),
			}))
			return
		} else {

			v, ok := this.store[msg.GetHead().UniKey]
			if !ok {
				v = &kv{
					uniKey:  head.UniKey,
					key:     key,
					table:   table,
					version: time.Now().UnixNano(),
					meta:    m,
					fields:  map[string]*proto.Field{},
				}

				fillDefaultValue(v.meta, &v.fields)
			}

			if !checkVersion(req.Version, v.version) {
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_VERSION_MISMATCH,
				}, &proto.SetResp{
					Version: req.GetVersion(),
				}))
			} else {
				v.version++

				for _, vv := range req.GetFields() {
					v.fields[vv.GetName()] = vv
				}

				this.store[head.UniKey] = v
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_OK,
				}, &proto.SetResp{
					Version: v.version,
				}))
			}
		}
	}
}

func (this *Node) setNx(session kendynet.StreamSession, msg *net.Message) {
	req := msg.GetData().(*proto.SetNxReq)
	head := msg.GetHead()

	if len(req.GetFields()) == 0 {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_MISSING_FIELDS,
		}, &proto.SetNxResp{
			Version: req.GetVersion(),
		}))
	} else {
		table, key := head.SplitUniKey()
		m := this.meta.GetTableMeta(table)
		if nil == m {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_INVAILD_TABLE,
			}, &proto.SetNxResp{
				Version: req.GetVersion(),
			}))
			return
		} else if !m.CheckSet(req.GetFields()) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_INVAILD_FIELD,
			}, &proto.SetNxResp{
				Version: req.GetVersion(),
			}))
			return
		} else {

			v, ok := this.store[msg.GetHead().UniKey]
			if ok {

				fields := []*proto.Field{}

				for _, field := range v.fields {
					fields = append(fields, field)
				}

				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_RECORD_EXIST,
				}, &proto.SetNxResp{
					Version: v.version,
					Fields:  fields,
				}))

			} else {

				v = &kv{
					uniKey:  head.UniKey,
					key:     key,
					table:   table,
					version: time.Now().UnixNano(),
					meta:    m,
					fields:  map[string]*proto.Field{},
				}

				fillDefaultValue(v.meta, &v.fields)

				v.version++

				for _, vv := range req.GetFields() {
					v.fields[vv.GetName()] = vv
				}

				this.store[head.UniKey] = v
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_OK,
				}, &proto.SetNxResp{
					Version: v.version,
				}))
			}
		}
	}
}

func (this *Node) compareAndSet(session kendynet.StreamSession, msg *net.Message) {

	req := msg.GetData().(*proto.CompareAndSetReq)

	head := msg.GetHead()

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_MISSING_FIELDS,
		}, &proto.CompareAndSetResp{
			Version: req.GetVersion(),
		}))

		return
	}

	table, _ := head.SplitUniKey()
	m := this.meta.GetTableMeta(table)
	if nil == m {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_TABLE,
		}, &proto.CompareAndSetResp{
			Version: req.GetVersion(),
		}))
		return
	} else if !m.CheckCompareAndSet(req.GetNew(), req.GetOld()) {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.CompareAndSetResp{
			Version: req.GetVersion(),
		}))
		return
	}

	v, ok := this.store[msg.GetHead().UniKey]
	if ok {
		if !checkVersion(req.Version, v.version) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_VERSION_MISMATCH,
			}, &proto.CompareAndSetResp{
				Version: v.version,
			}))
		} else {
			vv := v.fields[req.GetOld().GetName()]
			if !req.GetOld().IsEqual(vv) {
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_CAS_NOT_EQUAL,
				}, &proto.CompareAndSetResp{
					Version: v.version,
					Value:   vv,
				}))
			} else {
				v.fields[req.GetOld().GetName()] = req.GetNew()
				v.version++
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_OK,
				}, &proto.CompareAndSetResp{
					Version: v.version,
				}))
			}
		}

	} else {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_RECORD_NOTEXIST,
		}, &proto.CompareAndSetResp{
			Version: req.GetVersion(),
		}))
	}
}

func (this *Node) compareAndSetNx(session kendynet.StreamSession, msg *net.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := msg.GetHead()

	if nil == req.GetOld() || nil == req.GetNew() {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_MISSING_FIELDS,
		}, &proto.CompareAndSetNxResp{
			Version: req.GetVersion(),
		}))

		return
	}

	table, key := head.SplitUniKey()
	m := this.meta.GetTableMeta(table)
	if m == nil {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_TABLE,
		}, &proto.CompareAndSetNxResp{
			Version: req.GetVersion(),
		}))
		return
	} else if !m.CheckCompareAndSet(req.GetNew(), req.GetOld()) {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.CompareAndSetNxResp{
			Version: req.GetVersion(),
		}))
		return
	}

	v, ok := this.store[msg.GetHead().UniKey]

	if !ok {
		v = &kv{
			uniKey:  head.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*proto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		v.fields[req.GetOld().GetName()] = req.GetNew()

		v.version++

		this.store[head.UniKey] = v

		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_OK,
		}, &proto.CompareAndSetNxResp{
			Version: v.version,
		}))

	} else {

		if !checkVersion(req.Version, v.version) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_VERSION_MISMATCH,
			}, &proto.CompareAndSetNxResp{
				Version: v.version,
			}))
		} else {
			vv := v.fields[req.GetOld().GetName()]
			if !req.GetOld().IsEqual(vv) {
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_CAS_NOT_EQUAL,
				}, &proto.CompareAndSetNxResp{
					Version: v.version,
					Value:   vv,
				}))
			} else {
				v.fields[req.GetOld().GetName()] = req.GetNew()
				v.version++
				session.Send(net.NewMessage(net.CommonHead{
					Seqno:   head.Seqno,
					ErrCode: errcode.ERR_OK,
				}, &proto.CompareAndSetNxResp{
					Version: v.version,
				}))
			}
		}

	}
}

func (this *Node) incrBy(session kendynet.StreamSession, msg *net.Message) {

	req := msg.GetData().(*proto.IncrByReq)

	head := msg.GetHead()

	f := req.GetField()

	if nil == f {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.IncrByResp{
			Version: req.GetVersion(),
		}))
	}

	table, key := head.SplitUniKey()
	m := this.meta.GetTableMeta(table)
	if nil == m {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_TABLE,
		}, &proto.IncrByResp{
			Version: req.GetVersion(),
		}))
		return
	} else if !m.CheckField(f) {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.IncrByResp{
			Version: req.GetVersion(),
		}))
		return
	}

	v, ok := this.store[msg.GetHead().UniKey]

	if !ok {
		v = &kv{
			uniKey:  head.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*proto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		vv := v.fields[f.GetName()]

		v.fields[f.GetName()] = proto.PackField(f.GetName(), vv.GetInt()+f.GetInt())

		v.version++

		this.store[head.UniKey] = v

		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_OK,
		}, &proto.IncrByResp{
			Version: v.version,
			Field:   v.fields[f.GetName()],
		}))

	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_VERSION_MISMATCH,
			}, &proto.IncrByResp{
				Version: v.version,
			}))
		} else {

			vv := v.fields[f.GetName()]

			v.fields[f.GetName()] = proto.PackField(f.GetName(), vv.GetInt()+f.GetInt())

			v.version++

			this.store[head.UniKey] = v

			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_OK,
			}, &proto.IncrByResp{
				Version: v.version,
				Field:   v.fields[f.GetName()],
			}))

		}
	}
}

func (this *Node) decrBy(session kendynet.StreamSession, msg *net.Message) {
	req := msg.GetData().(*proto.DecrByReq)

	head := msg.GetHead()

	f := req.GetField()

	if nil == f {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.DecrByResp{
			Version: req.GetVersion(),
		}))
	}

	table, key := head.SplitUniKey()
	m := this.meta.GetTableMeta(table)
	if nil == m {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_TABLE,
		}, &proto.DecrByResp{
			Version: req.GetVersion(),
		}))
		return
	} else if !m.CheckField(f) {
		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_INVAILD_FIELD,
		}, &proto.DecrByResp{
			Version: req.GetVersion(),
		}))
		return
	}

	v, ok := this.store[msg.GetHead().UniKey]

	if !ok {
		v = &kv{
			uniKey:  head.UniKey,
			key:     key,
			table:   table,
			version: time.Now().UnixNano(),
			meta:    m,
			fields:  map[string]*proto.Field{},
		}

		fillDefaultValue(v.meta, &v.fields)

		vv := v.fields[f.GetName()]

		v.fields[f.GetName()] = proto.PackField(f.GetName(), vv.GetInt()-f.GetInt())

		v.version++

		this.store[head.UniKey] = v

		session.Send(net.NewMessage(net.CommonHead{
			Seqno:   head.Seqno,
			ErrCode: errcode.ERR_OK,
		}, &proto.DecrByResp{
			Version: v.version,
			Field:   v.fields[f.GetName()],
		}))

	} else {
		if !checkVersion(req.Version, v.version) {
			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_VERSION_MISMATCH,
			}, &proto.DecrByResp{
				Version: v.version,
			}))
		} else {

			vv := v.fields[f.GetName()]

			v.fields[f.GetName()] = proto.PackField(f.GetName(), vv.GetInt()-f.GetInt())

			v.version++

			this.store[head.UniKey] = v

			session.Send(net.NewMessage(net.CommonHead{
				Seqno:   head.Seqno,
				ErrCode: errcode.ERR_OK,
			}, &proto.DecrByResp{
				Version: v.version,
				Field:   v.fields[f.GetName()],
			}))

		}
	}
}

func (this *Node) kick(session kendynet.StreamSession, msg *net.Message) {
	session.Send(net.NewMessage(net.CommonHead{
		Seqno:   msg.GetHead().Seqno,
		ErrCode: errcode.ERR_OK,
	}, &proto.KickResp{}))
}

func (this *Node) initHandler() *Node {

	this.handlers = map[uint16]handler{}

	this.Register(uint16(protocol.CmdType_Del), this.del)
	this.Register(uint16(protocol.CmdType_Get), this.get)
	this.Register(uint16(protocol.CmdType_Set), this.set)
	this.Register(uint16(protocol.CmdType_SetNx), this.setNx)
	this.Register(uint16(protocol.CmdType_CompareAndSet), this.compareAndSet)
	this.Register(uint16(protocol.CmdType_CompareAndSetNx), this.compareAndSetNx)
	this.Register(uint16(protocol.CmdType_IncrBy), this.incrBy)
	this.Register(uint16(protocol.CmdType_DecrBy), this.decrBy)
	this.Register(uint16(protocol.CmdType_Kick), this.kick)

	return this

}

func New() *Node {
	n := &Node{store: map[string]*kv{}}
	return n.initHandler()
}
