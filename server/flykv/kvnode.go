package flykv

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

type kvnode struct {
	mu          sync.Mutex
	muC         sync.Mutex
	clients     map[*fnet.Socket]*fnet.Socket
	muS         sync.RWMutex
	stores      map[int]*kvstore
	running     int32
	config      *Config
	db          dbI
	meta        db.DBMeta
	listener    *cs.Listener
	id          int
	mutilRaft   *raft.MutilRaft
	stopOnce    int32
	startOnce   int32
	metaCreator func(*db.DbDef) (db.DBMeta, error)
	consoleConn *fnet.Udp
	selfUrl     string
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *kvnode) startListener() {
	this.listener.Serve(func(session *fnet.Socket) {
		go func() {

			session.SetUserData(
				&conn{
					session:    session,
					pendingCmd: map[int64]replyAble{},
				},
			)

			this.muC.Lock()
			this.clients[session] = session
			this.muC.Unlock()

			//session.SetRecvTimeout(flyproto.PingTime * 10)
			//session.SetSendQueueSize()

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetInBoundProcessor(cs.NewReqInboundProcessor())
			session.SetEncoder(&cs.RespEncoder{})
			session.SetCloseCallBack(func(session *fnet.Socket, reason error) {
				if u := session.GetUserData(); nil != u {
					switch u.(type) {
					case *conn:
						u.(*conn).clear()
					}
				}
				this.muC.Lock()
				delete(this.clients, session)
				this.muC.Unlock()
			})

			session.BeginRecv(func(session *fnet.Socket, v interface{}) {
				c := session.GetUserData()
				if nil == c {
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
				case flyproto.CmdType_Cancel:
					req := msg.Data.(*flyproto.Cancel)
					for _, v := range req.GetSeqs() {
						c.(*conn).removePendingCmdBySeqno(v)
					}
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
							from: c.(*conn),
							msg:  msg,
						})
					}
				}
			})
		}()
	})
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

func (this *kvnode) addStore(meta db.DBMeta, storeID int, cluster string, slots *bitmap.Bitmap) error {
	if atomic.LoadInt32(&this.running) == 0 {
		return errors.New("kvnode is not running")
	}

	clusterArray := strings.Split(cluster, ",")

	peers := map[int]string{}

	var selfUrl string

	for _, v := range clusterArray {
		t := strings.Split(v, "@")
		if len(t) != 2 {
			panic("invaild peer")
		}
		i, err := strconv.Atoi(t[0])
		if nil != err {
			panic(err)
		}
		peers[i] = t[1]
		if i == this.id {
			selfUrl = t[1]
		}
	}

	if selfUrl != this.selfUrl {
		return errors.New("cluster not contain self")
	}

	this.muS.Lock()
	defer this.muS.Unlock()

	_, ok := this.stores[storeID]
	if ok {
		return nil
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2),
	}

	var groupSize int = this.config.SnapshotCurrentCount

	if 0 == groupSize {
		groupSize = runtime.NumCPU()
	}

	store := &kvstore{
		db:         this.db,
		mainQueue:  mainQueue,
		keyvals:    make([]kvmgr, groupSize),
		kvnode:     this,
		shard:      storeID,
		slots:      slots,
		meta:       meta,
		memberShip: map[int]bool{},
		slotsKvMap: map[int]map[string]*kv{},
	}

	rn := raft.NewRaftNode(this.mutilRaft, mainQueue, (this.id<<16)+storeID, peers, false, this.config.Log.LogDir, "kvnode")

	store.rn = rn
	store.raftID = rn.ID()

	for i := 0; i < len(store.keyvals); i++ {
		store.keyvals[i].kv = map[string]*kv{}
		//store.keyvals[i].kicks = map[string]bool{}
	}

	store.lru.init()
	store.lease = newLease(store)
	store.memberShip[this.id] = true
	this.stores[storeID] = store
	store.serve()

	return nil
}

func (this *kvnode) Stop() {
	if atomic.CompareAndSwapInt32(&this.stopOnce, 0, 1) {
		this.mu.Lock()
		defer this.mu.Unlock()

		atomic.StoreInt32(&this.running, 0)
		//首先关闭监听,不在接受新到达的连接
		this.listener.Close()
		//关闭现有连接的读端，不会再接收新的req
		this.muC.Lock()
		for _, v := range this.clients {
			v.ShutdownRead()
		}
		this.muC.Unlock()

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
		clients := this.clients
		this.muC.Unlock()

		for _, v := range clients {
			v.Close(nil, time.Second*5)
		}

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

		if nil != this.consoleConn {
			this.consoleConn.Close()
		}

		this.db.stop()

	}
}

func makeStoreBitmap(stores []int) (b []*bitmap.Bitmap) {
	if len(stores) > 0 {
		slotPerStore := slot.SlotCount / len(stores)
		for i, _ := range stores {
			storeBitmap := bitmap.New(slot.SlotCount)
			j := i * slotPerStore
			for ; j < (i+1)*slotPerStore; j++ {
				storeBitmap.Set(j)
			}

			//不能正好平分，剩余的slot全部交给最后一个store
			if i == len(stores)-1 && j < slot.SlotCount {
				for ; j < slot.SlotCount; j++ {
					storeBitmap.Set(j)
				}
			}
			b = append(b, storeBitmap)
		}
	}
	return
}

func MakeUnikeyPlacement(stores []int) (fn func(string) int) {
	if len(stores) > 0 {
		slot2Store := map[int]int{}
		slotPerStore := slot.SlotCount / len(stores)
		for i, v := range stores {
			j := i * slotPerStore
			for ; j < (i+1)*slotPerStore; j++ {
				slot2Store[j] = v
			}

			//不能正好平分，剩余的slot全部交给最后一个store
			if i == len(stores)-1 && j < slot.SlotCount {
				for ; j < slot.SlotCount; j++ {
					slot2Store[j] = v
				}
			}
		}

		fn = func(unikey string) int {
			return slot2Store[slot.Unikey2Slot(unikey)]
		}
	}
	return
}

type storeConf struct {
	id          int
	raftCluster string
	slots       *bitmap.Bitmap
}

func (this *kvnode) getKvnodeBootInfo(serviceHost string, pd []*net.UDPAddr) *sproto.KvnodeBootResp {
	var resp *sproto.KvnodeBootResp

	for {
		respCh := make(chan *sproto.KvnodeBootResp)
		uu := make([]*fnet.Udp, len(pd))
		for k, v := range pd {
			u, err := fnet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
			if nil == err {
				uu[k] = u
				go func(u *fnet.Udp, pdAddr *net.UDPAddr) {
					u.SendTo(pdAddr, &sproto.KvnodeBoot{NodeID: int32(this.id), Host: serviceHost})
					recvbuff := make([]byte, 65535)
					_, r, err := u.ReadFrom(recvbuff)
					if nil == err {
						respCh <- r.(*sproto.KvnodeBootResp)
					}
				}(u, v)
			}
		}

		ticker := time.NewTicker(3 * time.Second)

		select {

		case v := <-respCh:
			resp = v
		case <-ticker.C:

		}

		ticker.Stop()

		for _, v := range uu {
			if nil != v {
				v.Close()
			}
		}

		if nil != resp {
			break
		}
	}

	return resp
}

func (this *kvnode) Start() error {
	var err error
	if atomic.CompareAndSwapInt32(&this.startOnce, 0, 1) {
		this.mu.Lock()
		defer this.mu.Unlock()

		config := this.config

		if err = os.MkdirAll(config.Log.LogDir, os.ModePerm); nil != err {
			return err
		}

		if config.Mode == "solo" {
			this.selfUrl = config.SoloConfig.RaftUrl

			err = this.db.start(config)

			if nil != err {
				return err
			}

			this.listener, err = cs.NewListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.SoloConfig.ServicePort), verifyLogin)

			if nil != err {
				return err
			}

			go this.mutilRaft.Serve(this.selfUrl)

			this.startListener()

			atomic.StoreInt32(&this.running, 1)

			//添加store
			if len(config.SoloConfig.Stores) > 0 {
				storeBitmaps := makeStoreBitmap(config.SoloConfig.Stores)
				for i, v := range config.SoloConfig.Stores {
					if err = this.addStore(this.meta, v, config.SoloConfig.RaftCluster, storeBitmaps[i]); nil != err {
						return err
					}
				}
			}

			GetSugar().Infof("flyfish start:%s:%d", config.ServiceHost, config.SoloConfig.ServicePort)

		} else {

			pd := strings.Split(config.ClusterConfig.PD, ";")

			var pdAddr []*net.UDPAddr

			for _, v := range pd {
				addr, err := net.ResolveUDPAddr("udp", v)
				if nil != err {
					return err
				} else {
					pdAddr = append(pdAddr, addr)
				}
			}

			resp := this.getKvnodeBootInfo(config.ServiceHost, pdAddr)

			if !resp.Ok {
				return errors.New(resp.Reason)
			}

			this.selfUrl = fmt.Sprintf("http://%s:%d", config.ServiceHost, resp.InterPort)

			err = this.initConsole(fmt.Sprintf("%s:%d", config.ServiceHost, resp.InterPort))

			if nil != err {
				return err
			}

			err = this.db.start(config)

			if nil != err {
				return err
			}

			this.listener, err = cs.NewListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, resp.ServicePort), verifyLogin)

			if nil != err {
				return err
			}

			go this.mutilRaft.Serve(this.selfUrl)

			this.startListener()

			atomic.StoreInt32(&this.running, 1)

			for _, v := range resp.Stores {
				slots, err := bitmap.CreateFromJson(v.Slots)

				if nil != err {
					return err
				}

				if err = this.addStore(this.meta, int(v.Id), v.RaftCluster, slots); nil != err {
					return err
				}
			}

			GetSugar().Infof("flyfish start:%s:%d", config.ServiceHost, resp.ServicePort)
		}

	}
	return err
}

func NewKvNode(id int, config *Config, metaDef *db.DbDef, metaCreator func(*db.DbDef) (db.DBMeta, error), db dbI) *kvnode {

	meta, err := metaCreator(metaDef)

	if nil != err {
		return nil
	}

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

	return &kvnode{
		id:          id,
		mutilRaft:   raft.NewMutilRaft(),
		clients:     map[*fnet.Socket]*fnet.Socket{},
		stores:      map[int]*kvstore{},
		db:          db,
		meta:        meta,
		metaCreator: metaCreator,
		config:      config,
	}
}
