package flykv

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *  这些预定义的Error类型，可以从其名字推出Desc,因此Desc全部设置为空字符串，以节省网络传输字节数
 */
var (
	Err_version_mismatch errcode.Error = errcode.New(errcode.Errcode_version_mismatch)
	Err_record_exist     errcode.Error = errcode.New(errcode.Errcode_record_exist)
	Err_record_notexist  errcode.Error = errcode.New(errcode.Errcode_record_notexist)
	Err_record_unchange  errcode.Error = errcode.New(errcode.Errcode_record_unchange)
	Err_cas_not_equal    errcode.Error = errcode.New(errcode.Errcode_cas_not_equal)
	Err_timeout          errcode.Error = errcode.New(errcode.Errcode_timeout)
)

type kvnode struct {
	muC       sync.Mutex
	clients   map[*fnet.Socket]struct{}
	muS       sync.RWMutex
	stores    map[int]*kvstore
	config    *Config
	dbc       *sqlx.DB
	db        dbI
	listener  *cs.Listener
	setID     int
	id        int
	mutilRaft *raft.MutilRaft
	closed    int32
	udpConn   *fnet.Udp
	join      bool
	pdAddr    []*net.UDPAddr
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *kvnode) onClient(session *fnet.Socket) {
	go func() {
		this.muC.Lock()
		this.clients[session] = struct{}{}
		this.muC.Unlock()

		session.SetRecvTimeout(flyproto.PingTime * 10)
		//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
		session.SetInBoundProcessor(cs.NewReqInboundProcessor())
		session.SetEncoder(&cs.RespEncoder{})
		session.SetCloseCallBack(func(session *fnet.Socket, reason error) {
			this.muC.Lock()
			delete(this.clients, session)
			this.muC.Unlock()
		})

		session.BeginRecv(func(session *fnet.Socket, v interface{}) {

			if atomic.LoadInt32(&this.closed) == 1 {
				return
			}

			msg := v.(*cs.ReqMessage)

			switch msg.Cmd {
			case flyproto.CmdType_Ping:
				session.Send(&cs.RespMessage{
					Cmd:   msg.Cmd,
					Seqno: msg.Seqno,
					Data: &flyproto.PingResp{
						Timestamp: time.Now().UnixNano(),
					},
				})
			default:
				this.muS.RLock()
				store, ok := this.stores[msg.Store]
				this.muS.RUnlock()
				if !ok {
					session.Send(&cs.RespMessage{
						Seqno: msg.Seqno,
						Cmd:   msg.Cmd,
						Err:   errcode.New(errcode.Errcode_error, fmt.Sprintf("%s not in current server", msg.UniKey)),
					})
				} else {
					store.addCliMessage(clientRequest{
						from: session,
						msg:  msg,
					})
				}
			}
		})
	}()
}

func (this *kvnode) startListener() {
	this.listener.Serve(this.onClient, this.onScanner)
}

func waitCondition(fn func() bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			if fn() {
				wg.Done()
				break
			}
		}
	}()
	wg.Wait()
}

func (this *kvnode) addStore(meta db.DBMeta, storeID int, peers map[uint16]raft.Member, slots *bitmap.Bitmap) error {

	this.muS.Lock()
	defer this.muS.Unlock()

	_, ok := this.stores[storeID]
	if ok {
		return nil
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, this.config.MainQueueMaxSize),
	}

	var groupSize int = this.config.SnapshotCurrentCount

	if 0 == groupSize {
		groupSize = runtime.NumCPU()
	}

	store := &kvstore{
		db:        this.db,
		mainQueue: mainQueue,
		kvnode:    this,
		shard:     storeID,
		meta:      meta,
		kvmgr: kvmgr{
			kv:               make([]map[string]*kv, groupSize),
			slotsKvMap:       map[int]map[string]*kv{},
			slots:            slots,
			slotsTransferOut: map[int]*SlotTransferProposal{},
		},
	}

	rn, err := raft.NewInstance(uint16(this.id), uint16(storeID), this.join, this.mutilRaft, mainQueue, peers, this.config.RaftLogDir, this.config.RaftLogPrefix)

	if nil != err {
		return err
	}

	store.rn = rn

	for i := 0; i < len(store.kv); i++ {
		store.kv[i] = map[string]*kv{}
	}

	store.lru.init()
	store.lease = newLease(store)
	this.stores[storeID] = store
	store.serve()

	//GetSugar().Infof("AddStore %v slots:%v", rn.ID().String(), slots.GetOpenBits())

	return nil
}

func (this *kvnode) Stop() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		this.listener.Close()

		//等待所有store响应处理请求以及回写完毕
		waitCondition(func() bool {
			this.muS.RLock()
			defer this.muS.RUnlock()
			for _, v := range this.stores {
				if atomic.LoadInt32(&v.wait4ReplyCount) != 0 || atomic.LoadInt32(&v.dbWriteBackCount) != 0 {
					return false
				}
			}
			return true
		})

		//关闭现有连接
		this.muC.Lock()
		for c, _ := range this.clients {
			go c.Close(nil, time.Second*5)
		}
		this.muC.Unlock()

		waitCondition(func() bool {
			this.muC.Lock()
			defer this.muC.Unlock()
			return len(this.clients) == 0
		})

		this.muS.RLock()
		for _, v := range this.stores {
			v.stop()
		}
		this.muS.RUnlock()

		waitCondition(func() bool {
			this.muS.RLock()
			defer this.muS.RUnlock()
			return len(this.stores) == 0
		})

		this.mutilRaft.Stop()

		if nil != this.udpConn {
			this.udpConn.Close()
		}

		this.db.stop()
		this.dbc.Close()

	}
}

func (this *kvnode) getKvnodeBootInfo(pd []*net.UDPAddr) (resp *sproto.KvnodeBootResp) {
	for {
		context := snet.MakeUniqueContext()
		r := snet.UdpCall(pd, snet.MakeMessage(context, &sproto.KvnodeBoot{NodeID: int32(this.id)}), time.Second, func(respCh chan interface{}, r interface{}) {
			if m, ok := r.(*snet.Message); ok {
				if resp, ok := m.Msg.(*sproto.KvnodeBootResp); ok && context == m.Context {
					select {
					case respCh <- resp:
					default:
					}
				}
			}
		})

		if nil != r {
			resp = r.(*sproto.KvnodeBootResp)
			return
		}
	}
}

var outputBufLimit fnet.OutputBufLimit = fnet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

func (this *kvnode) start() error {
	var meta db.DBMeta

	var dbdef *db.DbDef

	var err error

	config := this.config

	dbConfig := config.DBConfig

	this.dbc, err = sql.SqlOpen(config.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DB, dbConfig.User, dbConfig.Password)

	if nil != err {
		return err
	}

	defer func() {
		if nil != err {
			this.dbc.Close()
		}
	}()

	err = this.db.start(config, this.dbc)

	if nil != err {
		return err
	}

	if config.Mode == "solo" {

		f, err := os.Open(config.SoloConfig.MetaPath)
		if nil != err {
			return err
		}

		var b []byte
		for {
			data := make([]byte, 4096)
			n, err := f.Read(data)
			if n > 0 {
				b = append(b, data[:n]...)
			}

			if nil != err {
				break
			}
		}

		dbdef, err := db.MakeDbDefFromJsonString(b)
		if nil != err {
			return err
		}

		for _, v := range dbdef.TableDefs {
			tb, err := sql.GetTableScheme(this.dbc, config.DBType, fmt.Sprintf("%s_%d", v.Name, v.DbVersion))
			if nil != err {
				return err
			} else if nil == tb {
				//表不存在
				err = sql.CreateTables(this.dbc, config.DBType, v)
				if nil != err {
					return err
				} else {
					GetSugar().Infof("create table:%s_%d ok", v.Name, v.DbVersion)
				}
			} else {
				//记录字段的最大版本
				fields := map[string]*db.FieldDef{}
				for _, vv := range tb.Fields {
					f := fields[vv.Name]
					if nil == f || f.TabVersion <= vv.TabVersion {
						fields[vv.Name] = vv
					}
				}

				for _, vv := range v.Fields {
					f, ok := fields[vv.Name]
					if !ok {
						return fmt.Errorf("table:%s already in db but not match with meta,field:%s not found in db", f.Name, vv.Name)
					}

					if f.Type != vv.Type {
						return fmt.Errorf("table:%s already in db but not match with meta,field:%s type mismatch with db", f.Name, vv.Name)
					}

					if f.DefaultValue != vv.DefaultValue {
						GetSugar().Panicf("table:%s already in db but not match with meta,field:%s DefaultValue mismatch with db", f.Name, vv.Name)
					}
				}
			}
		}

		meta, err = sql.CreateDbMeta(dbdef)

		if nil != err {
			return err
		}

		service := fmt.Sprintf("%s:%d", config.SoloConfig.ServiceHost, config.SoloConfig.ServicePort)

		err = this.initUdp(service)

		if nil != err {
			return err
		}

		this.listener, err = cs.NewListener("tcp", service, outputBufLimit, verifyLogin)

		if nil != err {
			return err
		}

		go this.mutilRaft.Serve([]string{config.SoloConfig.RaftUrl})

		this.startListener()

		//添加store
		if len(config.SoloConfig.Stores) > 0 {
			peers, err := raft.SplitPeers(config.SoloConfig.RaftCluster)

			if nil != err {
				return err
			}

			storeBitmaps := sslot.MakeStoreBitmap(config.SoloConfig.Stores)
			for i, v := range config.SoloConfig.Stores {
				if err = this.addStore(meta, v, peers, storeBitmaps[i]); nil != err {
					return err
				}
			}
		}

		GetSugar().Infof("flyfish start:%s:%d", config.SoloConfig.ServiceHost, config.SoloConfig.ServicePort)

	} else {

		//meta从flypd获取

		pd := strings.Split(config.ClusterConfig.PD, ";")

		for _, v := range pd {
			addr, err := net.ResolveUDPAddr("udp", v)
			if nil != err {
				return err
			} else {
				this.pdAddr = append(this.pdAddr, addr)
			}
		}

		resp := this.getKvnodeBootInfo(this.pdAddr)

		if !resp.Ok {
			GetSugar().Errorf("getKvnodeBootInfo err:%v", resp.Reason)
			return errors.New(resp.Reason)
		}

		this.setID = int(resp.SetID)

		if dbdef, err = db.MakeDbDefFromJsonString(resp.Meta); nil != err {
			GetSugar().Errorf("CreateDbDefFromJsonString err:%v", err)
			return err
		}

		meta, err = sql.CreateDbMeta(dbdef)

		if nil != err {
			GetSugar().Errorf("CreateDbMeta err:%v", err)
			return err
		}

		err = this.initUdp(fmt.Sprintf("%s:%d", resp.ServiceHost, resp.ServicePort))

		if nil != err {
			GetSugar().Errorf("initUdp err:%v", err)
			return err
		}

		service := fmt.Sprintf("%s:%d", resp.ServiceHost, resp.ServicePort)

		this.listener, err = cs.NewListener("tcp", service, outputBufLimit, verifyLogin)

		if nil != err {
			GetSugar().Errorf("NewListener err:%v", err)
			return err
		}

		go this.mutilRaft.Serve([]string{fmt.Sprintf("http://%s:%d", resp.ServiceHost, resp.RaftPort)})

		this.startListener()

		//cluster模式下membership由pd负责管理,节点每次启动从pd获取
		for _, v := range resp.Stores {
			slots, err := bitmap.CreateFromJson(v.Slots)

			if nil != err {
				GetSugar().Errorf("CreateFromJson err:%v", err)
				return err
			}

			peers, err := raft.SplitPeers(v.RaftCluster)

			if nil != err {
				GetSugar().Errorf("SplitPeers err:%v,origin:%v", err, v.RaftCluster)
				return err
			}

			if err = this.addStore(meta, int(v.Id), peers, slots); nil != err {
				GetSugar().Errorf("addStore err:%v", err)
				return err
			}
		}

		GetSugar().Infof("flyfish start:%s:%d", resp.ServiceHost, resp.ServicePort)
	}

	return err
}

func NewKvNode(id int, join bool, config *Config, db dbI) (*kvnode, error) {

	if config.ProposalFlushInterval > 0 {
		raft.ProposalFlushInterval = config.ProposalFlushInterval
	}

	if config.ReadFlushInterval > 0 {
		raft.ReadFlushInterval = config.ReadFlushInterval
	}

	if config.ProposalBatchCount > 0 {
		raft.ProposalBatchCount = config.ProposalBatchCount
	}

	if config.ReadBatchCount > 0 {
		raft.ReadBatchCount = config.ReadBatchCount
	}

	if config.StoreReqLimit.SoftLimit <= 0 {
		config.StoreReqLimit.SoftLimit = 20000
	}

	if config.StoreReqLimit.HardLimit <= 0 {
		config.StoreReqLimit.HardLimit = 50000
	}

	if config.StoreReqLimit.SoftLimitSeconds <= 0 {
		config.StoreReqLimit.SoftLimitSeconds = 10
	}

	node := &kvnode{
		id:        id,
		mutilRaft: raft.NewMutilRaft(),
		clients:   map[*fnet.Socket]struct{}{},
		stores:    map[int]*kvstore{},
		db:        db,
		config:    config,
		join:      join,
	}

	err := node.start()

	return node, err
}
