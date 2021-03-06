package kvnode

import (
	//"errors"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/server/slot"
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
	mu sync.Mutex

	muC     sync.Mutex
	clients map[*net.Socket]*net.Socket

	muS     sync.RWMutex
	stores  map[int]*kvstore
	running int32
	config  *Config

	db          dbbackendI
	meta        db.DBMeta
	listener    *cs.Listener
	id          int
	mutilRaft   *raft.MutilRaft
	stopOnce    sync.Once
	startOnce   sync.Once
	metaCreator func(*db.DbDef) (db.DBMeta, error)

	selfUrl string
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *kvnode) startListener() {
	this.listener.Serve(func(session *net.Socket) {
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

			session.SetRecvTimeout(flyproto.PingTime * 10)
			session.SetSendQueueSize(10000)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetInBoundProcessor(cs.NewReqInboundProcessor())
			session.SetEncoder(&cs.RespEncoder{})
			session.SetCloseCallBack(func(session *net.Socket, reason error) {
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

			session.BeginRecv(func(session *net.Socket, v interface{}) {
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

func (this *kvnode) remStore(storeID int) error {
	if atomic.LoadInt32(&this.running) == 0 {
		return errors.New("kvnode is not running")
	}

	this.muS.Lock()

	s, ok := this.stores[storeID]
	if !ok {
		return nil
	}

	this.muS.Unlock()

	s.removeonce.Do(func() {

		s.mainQueue.AppendHighestPriotiryItem(func() {
			s.removing = true
		})

		go func() {
			waitCondition(func() bool {
				if atomic.LoadInt32(&s.wait4ReplyCount) != 0 {
					return false
				}
				return true
			})
			s.stop()
			this.muS.Lock()
			delete(this.stores, storeID)
			this.muS.Unlock()

		}()
	})

	return nil
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
		db:        this.db,
		mainQueue: mainQueue,
		keyvals:   make([]kvmgr, groupSize),
		kvnode:    this,
		shard:     storeID,
		slots:     slots,
		meta:      meta,
	}

	rn := raft.NewRaftNode(store.snapMerge, this.mutilRaft, mainQueue, (this.id<<16)+storeID, peers, false, this.config.Log.LogDir, "kvnode")

	store.rn = rn
	store.raftID = rn.ID()

	for i := 0; i < len(store.keyvals); i++ {
		store.keyvals[i].kv = map[string]*kv{}
		store.keyvals[i].kicks = map[string]bool{}
	}

	store.lru.init()
	store.lease = newLease(store)
	this.stores[storeID] = store
	store.serve()

	return nil
}

func (this *kvnode) Stop() {
	this.stopOnce.Do(func() {
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

		//等待所有store响应处理请求
		waitCondition(func() bool {
			this.muS.RLock()
			defer this.muS.RUnlock()
			for _, v := range this.stores {
				if atomic.LoadInt32(&v.wait4ReplyCount) != 0 {
					return false
				}
			}
			return true
		})

		this.db.stop()

		//关闭现有连接
		this.muC.Lock()
		for _, v := range this.clients {
			v.Close(nil, time.Second*5)
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

	})
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

func (this *kvnode) Start() error {
	var err error
	this.startOnce.Do(func() {
		this.mu.Lock()
		defer this.mu.Unlock()

		config := this.config

		if err = os.MkdirAll(config.Log.LogDir, os.ModePerm); nil != err {
			return
		}

		if config.Mode == "solo" {
			this.selfUrl = config.RaftUrl

			err = this.db.start(config)

			if nil != err {
				return
			}

			this.listener, err = cs.NewListener("tcp", fmt.Sprintf("%s:%d", config.SoloConfig.ServiceHost, config.SoloConfig.ServicePort), verifyLogin)

			if nil != err {
				return
			}

			go this.mutilRaft.Serve(this.selfUrl)

			this.startListener()

			atomic.StoreInt32(&this.running, 1)

			//添加store
			if len(config.SoloConfig.Stores) > 0 {
				storeBitmaps := makeStoreBitmap(config.SoloConfig.Stores)
				for i, v := range config.SoloConfig.Stores {
					if err = this.addStore(this.meta, v, config.SoloConfig.RaftCluster, storeBitmaps[i]); nil != err {
						return
					}
				}
			}

			GetSugar().Infof("flyfish start:%s:%d", config.SoloConfig.ServiceHost, config.SoloConfig.ServicePort)

		} else {

		}

	})
	return err
}

func NewKvNode(id int, config *Config, metaDef *db.DbDef, metaCreator func(*db.DbDef) (db.DBMeta, error), db dbbackendI) *kvnode {

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
		clients:     map[*net.Socket]*net.Socket{},
		stores:      map[int]*kvstore{},
		db:          db,
		meta:        meta,
		metaCreator: metaCreator,
		config:      config,
	}
}
