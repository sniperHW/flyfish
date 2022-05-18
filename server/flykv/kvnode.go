package flykv

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/list"
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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *  这些预定义的Error类型，可以从其名字推出Desc,因此Desc全部设置为空字符串，以节省网络传输字节数
 */
var (
	Err_not_leader       errcode.Error = errcode.New(errcode.Errcode_not_leader, "Errcode_not_leader")
	Err_version_mismatch errcode.Error = errcode.New(errcode.Errcode_version_mismatch, "Errcode_version_mismatch")
	Err_record_exist     errcode.Error = errcode.New(errcode.Errcode_record_exist, "Errcode_record_exist")
	Err_record_notexist  errcode.Error = errcode.New(errcode.Errcode_record_notexist, "Errcode_record_notexist")
	Err_record_unchange  errcode.Error = errcode.New(errcode.Errcode_record_unchange, "Errcode_record_unchange")
	Err_cas_not_equal    errcode.Error = errcode.New(errcode.Errcode_cas_not_equal, "Errcode_cas_not_equal")
	Err_timeout          errcode.Error = errcode.New(errcode.Errcode_timeout, "Errcode_timeout")
)

type writeBackMode byte

const (
	write_back_on_swap = writeBackMode(1)
	write_through      = writeBackMode(2)
)

type replyer struct {
	session         *fnet.Socket
	seqno           int64
	totalPendingReq *int64
}

func (r *replyer) reply(resp *cs.RespMessage) {
	atomic.AddInt64(r.totalPendingReq, -1)
	if nil != r.session {
		r.session.Send(resp)
	}
}

func (r *replyer) dropReply() {
	atomic.AddInt64(r.totalPendingReq, -1)
}

type kvnode struct {
	muC                  sync.Mutex
	clients              map[*fnet.Socket]struct{}
	muS                  sync.RWMutex
	stores               map[int]*kvstore
	config               *Config
	dbc                  *sqlx.DB
	db                   dbI
	listener             *cs.Listener
	setID                int
	id                   uint16
	mutilRaft            *raft.MutilRaft
	closed               int32
	udpConn              *fnet.Udp
	join                 bool
	pdAddr               []*net.UDPAddr
	writeBackMode        writeBackMode
	totalPendingReq      int64
	SoftLimitReachedTime int64
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *kvnode) makeReplyer(session *fnet.Socket, req *cs.ReqMessage, checklimit bool) *replyer {
	c := atomic.AddInt64(&this.totalPendingReq, 1)
	replyer := &replyer{
		session:         session,
		totalPendingReq: &this.totalPendingReq,
		seqno:           req.Seqno,
	}
	if !checklimit || this.checkReqLimit(int(c)) {
		return replyer
	} else {
		replyer.reply(&cs.RespMessage{
			Cmd:   req.Cmd,
			Seqno: req.Seqno,
			Err:   errcode.New(errcode.Errcode_retry, "flykv busy,please retry later"),
		})
		return nil
	}
}

func (this *kvnode) checkReqLimit(c int) bool {
	conf := this.config.ReqLimit

	if c > conf.HardLimit {
		return false
	}

	if c > conf.SoftLimit {
		nowUnix := time.Now().Unix()
		if !atomic.CompareAndSwapInt64(&this.SoftLimitReachedTime, 0, nowUnix) {
			SoftLimitReachedTime := atomic.LoadInt64(&this.SoftLimitReachedTime)
			if SoftLimitReachedTime > 0 && int(nowUnix-SoftLimitReachedTime) >= conf.SoftLimitSeconds {
				return false
			}
		}
	} else if SoftLimitReachedTime := atomic.LoadInt64(&this.SoftLimitReachedTime); SoftLimitReachedTime > 0 {
		atomic.CompareAndSwapInt64(&this.SoftLimitReachedTime, SoftLimitReachedTime, 0)
	}

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

			//GetSugar().Infof("recv msg node:%d %v", this.id, v.(*cs.ReqMessage).Cmd)

			if atomic.LoadInt32(&this.closed) == 1 {
				GetSugar().Infof("kvnode closed")
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

				replyer := this.makeReplyer(session, msg, true)

				if nil == replyer {
					return
				}

				this.muS.RLock()
				store, ok := this.stores[msg.Store]
				this.muS.RUnlock()
				if !ok {
					replyer.reply(&cs.RespMessage{
						Seqno: msg.Seqno,
						Cmd:   msg.Cmd,
						Err:   errcode.New(errcode.Errcode_error, fmt.Sprintf("%s not in current server", msg.UniKey)),
					})
				} else {
					store.addCliMessage(clientRequest{
						replyer: replyer,
						msg:     msg,
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

	store := &kvstore{
		db:        this.db,
		mainQueue: mainQueue,
		kvnode:    this,
		shard:     storeID,
		meta:      meta,
		slotMgr: slotMgr{
			slots:            make([]*slot, sslot.SlotCount),
			slotsTransferOut: map[int]bool{},
			kickableList:     list.New(),
			hardkvlimited:    (this.config.MaxCachePerStore * 3) / 2,
		},
	}

	for _, v := range slots.GetOpenBits() {
		store.slots[v] = &slot{
			kvMap: map[string]*kv{},
		}
	}

	rn, err := raft.NewInstance(this.id, storeID, this.join, this.mutilRaft, mainQueue, peers, raft.RaftInstanceOption{
		SnapshotCount:           this.config.SnapshotCount,
		SnapshotCatchUpEntriesN: this.config.SnapshotCatchUpEntriesN,
		SnapshotBytes:           this.config.SnapshotBytes,
		MaxBatchCount:           this.config.MaxBatchCount,
		Logdir:                  this.config.RaftLogDir,
		RaftLogPrefix:           this.config.RaftLogPrefix,
	})

	if nil != err {
		return err
	}

	store.rn = rn

	this.stores[storeID] = store
	store.serve()

	return nil
}

func (this *kvnode) Stop() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		//首先关闭监听,不在接受新到达的连接
		this.listener.Close()

		waitCondition(func() bool {
			return atomic.LoadInt64(&this.totalPendingReq) == 0
		})

		//等待所有store回写完毕
		waitCondition(func() bool {
			this.muS.RLock()
			defer this.muS.RUnlock()
			for _, v := range this.stores {
				if v.isLeader() && atomic.LoadInt32(&v.dbWriteBackCount) != 0 {
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

	this.dbc, err = sql.SqlOpen(dbConfig.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DB, dbConfig.User, dbConfig.Password)

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

		err = sql.CreateBloomFilter(dbConfig.DBType, this.dbc)

		if nil != err {
			return err
		}

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

		for _, t := range dbdef.TableDefs {
			tb, err := sql.GetTableScheme(this.dbc, dbConfig.DBType, fmt.Sprintf("%s_%d", t.Name, t.DbVersion))
			if nil != err {
				return err
			} else if nil == tb {
				//表不存在
				err = sql.CreateTables(this.dbc, dbConfig.DBType, t)
				if nil != err {
					return err
				} else {
					GetSugar().Infof("create table:%s_%d ok", t.Name, t.DbVersion)
				}
			} else {
				//记录字段的最大版本
				fields := map[string]*db.FieldDef{}
				for _, v := range tb.Fields {
					f := fields[v.Name]
					if nil == f || f.TabVersion <= v.TabVersion {
						fields[v.Name] = v
					}
				}

				for _, v := range t.Fields {
					f, ok := fields[v.Name]
					if !ok {
						GetSugar().Panicf("table:%s already in db but not match with meta,field:%s not found in db", t.Name, v.Name)
					}

					if f.Type != v.Type {
						GetSugar().Panicf("table:%s already in db but not match with meta,field:%s type mismatch with db", t.Name, v.Name)
					}

					if f.GetDefaultValueStr() != v.GetDefaultValueStr() {
						GetSugar().Panicf("table:%s already in db but not match with meta,field:%s DefaultValue mismatch with db db.v:%v meta.v:%v", t.Name, v.Name, f.GetDefaultValueStr(), v.GetDefaultValueStr())
					}
				}
			}
		}

		meta, err = sql.CreateDbMeta(dbdef)

		if nil != err {
			return err
		}

		peers, err := raft.SplitPeers(config.SoloConfig.RaftUrl)

		if nil != err {
			return err
		}

		self, ok := peers[this.id]

		if !ok {
			return errors.New("rafturl not contain self")
		}

		service := self.ClientURL

		err = this.initUdp(service)

		if nil != err {
			return err
		}

		this.listener, err = cs.NewListener("tcp", service, outputBufLimit, verifyLogin)

		if nil != err {
			return err
		}

		go this.mutilRaft.Serve([]string{self.URL})

		this.startListener()

		//添加store
		if len(config.SoloConfig.Stores) > 0 {
			storeBitmaps := sslot.MakeStoreBitmap(config.SoloConfig.Stores)
			for i, v := range config.SoloConfig.Stores {
				for k, vv := range peers {
					vv.ID = uint64(vv.ProcessID)<<32 + uint64(v)
					peers[k] = vv
				}
				if err = this.addStore(meta, v, peers, storeBitmaps[i]); nil != err {
					return err
				}
			}
		}

		GetSugar().Infof("flyfish start:%s", service)

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

		var resp *sproto.KvnodeBootResp

		for {
			resp = this.getKvnodeBootInfo(this.pdAddr)
			if !resp.Ok {
				GetSugar().Errorf("getKvnodeBootInfo err:%v", resp.Reason)
				return errors.New(resp.Reason)
			}

			if len(resp.Stores) > 0 {
				break
			} else {
				time.Sleep(time.Second)
			}
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

		GetSugar().Infof("flyfish start:%s", service)

		go this.reportStatus()
	}

	return err
}

func (this *kvnode) reportStatus() {
	if atomic.LoadInt32(&this.closed) == 1 {
		return
	}

	report := &sproto.KvnodeReportStatus{
		SetID:  int32(this.setID),
		NodeID: int32(this.id),
	}

	wait := sync.WaitGroup{}

	this.muS.RLock()
	if len(this.stores) > 0 {
		mu := sync.Mutex{}
		wait.Add(len(this.stores))
		for _, v := range this.stores {
			store := v
			store.mainQueue.AppendHighestPriotiryItem(func() {
				mu.Lock()
				report.Stores = append(report.Stores, &sproto.StoreReportStatus{
					StoreID:     int32(store.shard),
					Isleader:    store.isLeader(),
					Kvcount:     int32(store.kvcount),
					Progress:    store.rn.GetApplyIndex(),
					MetaVersion: store.meta.GetVersion(),
					RaftID:      store.rn.ID(),
					Halt:        store.halt,
				})
				mu.Unlock()
				wait.Done()
			})
		}
	}
	this.muS.RUnlock()

	wait.Wait()

	GetSugar().Debugf("node:%d reportStatus store count:%d", this.id, len(report.Stores))

	for _, v := range this.pdAddr {
		this.udpConn.SendTo(v, snet.MakeMessage(0, report))
	}

	time.AfterFunc(time.Second, this.reportStatus)

}

func NewKvNode(id uint16, join bool, config *Config, db dbI) (*kvnode, error) {
	if config.ReqLimit.SoftLimit <= 0 {
		config.ReqLimit.SoftLimit = 100000
	}

	if config.ReqLimit.HardLimit <= 0 {
		config.ReqLimit.HardLimit = 150000
	}

	if config.ReqLimit.SoftLimitSeconds <= 0 {
		config.ReqLimit.SoftLimitSeconds = 10
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

	if config.WriteBackMode == "WriteThrough" {
		node.writeBackMode = write_through
	} else {
		node.writeBackMode = write_back_on_swap
	}

	err := node.start()

	return node, err
}
