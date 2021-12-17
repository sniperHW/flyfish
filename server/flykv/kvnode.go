package flykv

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"go.etcd.io/etcd/pkg/types"
	"net"
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
	muC         sync.Mutex
	clients     map[*fnet.Socket]struct{}
	muS         sync.RWMutex
	stores      map[int]*kvstore
	config      *Config
	db          dbI
	listener    *cs.Listener
	id          int
	mutilRaft   *raft.MutilRaft
	stopOnce    int32
	startOnce   int32
	udpConn     *fnet.Udp
	soloStorage Storage //for solo mode use only
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (this *kvnode) startListener() {
	this.listener.Serve(func(session *fnet.Socket) {
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

				if atomic.LoadInt32(&this.stopOnce) == 1 {
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

func (this *kvnode) addStore(meta db.DBMeta, storeID int, mb *membership.MemberShip, slots *bitmap.Bitmap) error {
	if nil == mb.Member(types.ID(raft.MakeInstanceID(uint16(this.id), uint16(storeID)))) {
		return errors.New("member not contain self")
	}

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

	rn := raft.NewInstance(uint16(this.id), uint16(storeID), this.mutilRaft, mainQueue, mb, false, this.config.RaftLogDir, this.config.RaftLogPrefix)

	store.rn = rn

	for i := 0; i < len(store.kv); i++ {
		store.kv[i] = map[string]*kv{}
	}

	store.lru.init()
	store.lease = newLease(store)
	this.stores[storeID] = store
	store.serve()

	return nil
}

func (this *kvnode) Stop() {
	if atomic.CompareAndSwapInt32(&this.stopOnce, 0, 1) {
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

func (this *kvnode) getKvnodeBootInfo(pd []*net.UDPAddr) *sproto.KvnodeBootResp {
	var resp *sproto.KvnodeBootResp

	for {
		respCh := make(chan *sproto.KvnodeBootResp)
		uu := make([]*fnet.Udp, len(pd))
		context := snet.MakeUniqueContext()
		for k, v := range pd {
			u, err := fnet.NewUdp(fmt.Sprintf(":0"), snet.Pack, snet.Unpack)
			if nil == err {
				uu[k] = u
				go func(u *fnet.Udp, pdAddr *net.UDPAddr) {
					u.SendTo(pdAddr, snet.MakeMessage(context, &sproto.KvnodeBoot{NodeID: int32(this.id)}))
					recvbuff := make([]byte, 65535)
					_, r, err := u.ReadFrom(recvbuff)
					if nil == err {
						if m, ok := r.(*snet.Message); ok && context == m.Context {
							respCh <- m.Msg.(*sproto.KvnodeBootResp)
						}
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

var outputBufLimit fnet.OutputBufLimit = fnet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

func (this *kvnode) Start() error {
	var err error
	if atomic.CompareAndSwapInt32(&this.startOnce, 0, 1) {

		var meta db.DBMeta

		var dbdef *db.DbDef

		var err error

		config := this.config

		if config.Mode == "solo" {

			if meta, err = this.soloStorage.LoadMeta(GetLogger()); nil != err {
				return err
			}

			if nil == meta {
				if dbdef, err = db.CreateDbDefFromCsv(config.SoloConfig.Meta); nil != err {
					return err
				}

				meta, err = sql.CreateDbMeta(1, dbdef)

				if nil != err {
					return err
				}

				if j, err := meta.ToJson(); nil != err {
					return err
				} else if err = this.soloStorage.SaveMeta(GetLogger(), j); nil != err {
					return err
				}
			}

			err = this.db.start(config)

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
				clusterArray := strings.Split(config.SoloConfig.RaftCluster, ",")
				peers := map[int]string{}
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
				}

				storeBitmaps := makeStoreBitmap(config.SoloConfig.Stores)
				for i, v := range config.SoloConfig.Stores {
					//首先尝试从storage加载membership
					mb, err := this.soloStorage.LoadMemberShip(GetLogger(), types.ID(v), types.ID(this.id))
					if nil != err {
						return err
					}

					if nil == mb {
						//storage中没有从配置文件中创建
						membs := []*membership.Member{}
						for kk, vv := range peers {
							u, _ := types.NewURLs([]string{vv})
							membs = append(membs, membership.NewMember(types.ID(raft.MakeInstanceID(uint16(kk), uint16(v))), u))
						}
						mb = membership.NewMemberShipMembers(GetLogger(), types.ID(this.id), types.ID(v), membs)
						mb.SetStorage(this.soloStorage)
						mb.Save()
					}

					if err = this.addStore(meta, v, mb, storeBitmaps[i]); nil != err {
						return err
					}
				}
			}

			GetSugar().Infof("flyfish start:%s:%d", config.SoloConfig.ServiceHost, config.SoloConfig.ServicePort)

		} else {

			//meta从flypd获取

			/*			pd := strings.Split(config.ClusterConfig.PD, ";")

						var pdAddr []*net.UDPAddr

						for _, v := range pd {
							addr, err := net.ResolveUDPAddr("udp", v)
							if nil != err {
								return err
							} else {
								pdAddr = append(pdAddr, addr)
							}
						}

						resp := this.getKvnodeBootInfo(pdAddr)

						if !resp.Ok {
							return errors.New(resp.Reason)
						}

						if dbdef, err = db.CreateDbDefFromJsonString(resp.Meta); nil != err {
							return err
						}

						meta, err = sql.CreateDbMeta(resp.MetaVersion, dbdef)

						if nil != err {
							return err
						}

						this.selfUrl = fmt.Sprintf("http://%s:%d", resp.ServiceHost, resp.RaftPort)

						err = this.initUdp(fmt.Sprintf("%s:%d", resp.ServiceHost, resp.ServicePort))

						if nil != err {
							return err
						}

						err = this.db.start(config)

						if nil != err {
							return err
						}

						service := fmt.Sprintf("%s:%d", resp.ServiceHost, resp.ServicePort)

						this.listener, err = cs.NewListener("tcp", service, outputBufLimit, verifyLogin)

						if nil != err {
							return err
						}

						go this.mutilRaft.Serve(this.selfUrl)

						this.startListener()

						for _, v := range resp.Stores {
							slots, err := bitmap.CreateFromJson(v.Slots)

							if nil != err {
								return err
							}

							if err = this.addStore(meta, int(v.Id), v.RaftCluster, slots); nil != err {
								return err
							}
						}

						GetSugar().Infof("flyfish start:%s:%d", resp.ServiceHost, resp.ServicePort)*/
		}

	}
	return err
}

func NewKvNode(id int, config *Config, db dbI, s Storage) *kvnode {

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

	return &kvnode{
		id:          id,
		mutilRaft:   raft.NewMutilRaft(),
		clients:     map[*fnet.Socket]struct{}{},
		stores:      map[int]*kvstore{},
		db:          db,
		config:      config,
		soloStorage: s,
	}
}
