package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/compress"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/pkg/timer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	//"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"math/rand"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type jsonKvnode struct {
	Id          int    //nodeid
	Service     string //对外服务地址端口
	RaftService string //raft服务地址
	UdpService  string
	Stores      []int
}

type jsonStore struct {
	Id       int
	Slots    []byte
	Removing bool
}

type jsonSnapShot struct {
	Kvnodes               []jsonKvnode
	Stores                []jsonStore
	SlotTransactions      []*slotTransferTransaction
	NodeStoreTransactions []*nodeStoreTransaction
}

type kvnode struct {
	id          int    //nodeid
	service     string //对外服务地址端口
	raftService string //raft服务地址
	stores      map[int]*store
	udpService  string
	udpAddr     *net.UDPAddr
}

type store struct {
	id       int            //store id
	slots    *bitmap.Bitmap //slots bitmap
	removing bool           //store正在被删除

	clusterStr string
	kvnodes    map[int]*kvnode //这个store的raft group
}

type applicationQueue struct {
	q *queue.PriorityQueue
}

type pd struct {
	raftID     int
	leader     int
	rn         *raft.RaftNode
	mutilRaft  *raft.MutilRaft
	mainque    applicationQueue
	udp        *flynet.Udp
	stores     map[int]*store
	kvnodes    map[int]*kvnode
	slot2store map[int32]*store

	msgHandler map[reflect.Type]func(*net.UDPAddr, proto.Message)

	nextTransID       int64
	transNodeStore    map[int64]*nodeStoreTransaction
	transSlotTransfer map[int64]*slotTransferTransaction

	tmpTransNodeStore map[int64]*nodeStoreTransaction
	transferingSlot   map[int32]bool //正在迁移中的slot

	stoponce  int32
	startonce int32
	wait      sync.WaitGroup
	ready     bool
}

func NewPd(udpService string, id int, cluster string) (*pd, error) {
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
		if i == id {
			selfUrl = t[1]
		}
	}

	if selfUrl == "" {
		return nil, errors.New("cluster not contain self")
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, GetConfig().MainQueueMaxSize),
	}

	mutilRaft := raft.NewMutilRaft()

	rn := raft.NewRaftNode(snapMerge, mutilRaft, mainQueue, (id<<16)+1, peers, false, GetConfig().Log.LogDir, "pd")

	p := &pd{
		rn:                rn,
		mainque:           mainQueue,
		raftID:            rn.ID(),
		mutilRaft:         mutilRaft,
		stores:            map[int]*store{},
		kvnodes:           map[int]*kvnode{},
		slot2store:        map[int32]*store{},
		msgHandler:        map[reflect.Type]func(*net.UDPAddr, proto.Message){},
		transNodeStore:    map[int64]*nodeStoreTransaction{},
		transSlotTransfer: map[int64]*slotTransferTransaction{},
		transferingSlot:   map[int32]bool{},
		tmpTransNodeStore: map[int64]*nodeStoreTransaction{},
	}

	p.initMsgHandler()

	if err := p.startUdpService(udpService); nil != err {
		rn.Stop()
		return nil, err
	}

	GetSugar().Infof("mutilRaft serve on:%s", selfUrl)

	go p.mutilRaft.Serve(selfUrl)

	p.wait.Add(1)
	go p.serve()

	return p, nil

}

func (s *store) updateClusterStr() {
	clusterStr := ""
	for _, vv := range s.kvnodes {
		if clusterStr != "" {
			clusterStr += ","
		}
		clusterStr += fmt.Sprintf("%d@%s", vv.id, vv.raftService)
	}
	s.clusterStr = clusterStr
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	if err := q.q.ForceAppend(1, m); nil != err {
		panic(err)
	}
}

func (q applicationQueue) append(m interface{}) error {
	return q.q.ForceAppend(0, m)
}

func (q applicationQueue) pop() (closed bool, v interface{}) {
	return q.q.Pop()
}

func (q applicationQueue) close() {
	q.q.Close()
}

func (p *pd) isLeader() bool {
	return p.leader == p.raftID
}

func (p *pd) issueProposal(proposal raft.Proposal) error {
	return p.rn.IssueProposal(proposal)
}

func (p *pd) startUdpService(udpService string) error {
	udp, err := flynet.NewUdp(udpService, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	p.udp = udp

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := udp.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				GetSugar().Infof("got msg")
				p.mainque.append(func() {
					if p.isLeader() && p.ready {
						p.onMsg(from, msg)
					} else {
						GetSugar().Infof("drop msg")
					}
				})
			}
		}
	}()

	return nil
}

func (p *pd) initPd() {
	stores := []*store{}
	for _, v := range GetConfig().Stores {
		for _, vv := range stores {
			if vv.id == v {
				panic(fmt.Sprintf("repeated store:%d", v))
			}
		}

		stores = append(stores, &store{
			id:    v,
			slots: bitmap.New(slot.SlotCount),
		})
	}

	//将slots随机分配给创世store
	for i := 0; i < slot.SlotCount; i++ {
		stores[rand.Int()%len(stores)].slots.Set(i)
	}

	initPd := initPd{}

	checkKvnode := func(v node) error {
		//重复检查
		for _, vv := range initPd.Kvnodes {
			if v.Id == vv.Id {
				return errors.New(fmt.Sprintf("repeated node:%d", v.Id))
			}

			if v.Service == vv.Service {
				return errors.New(fmt.Sprintf("repeated service:%s", v.Service))
			}

			if v.RaftService == vv.RaftService {
				return errors.New(fmt.Sprintf("repeated RaftService:%s", v.RaftService))
			}

			if v.UdpService == vv.UdpService {
				return errors.New(fmt.Sprintf("repeated UdpService:%s", v.UdpService))
			}

		}

		//服务地址检查
		var err error

		_, err = url.Parse(v.RaftService)
		if nil != err {
			return err
		}

		_, err = net.ResolveUDPAddr("udp", v.UdpService)
		if nil != err {
			return err
		}

		_, err = net.ResolveTCPAddr("tcp", v.Service)
		if nil != err {
			return err
		}

		return nil
	}

	for _, v := range GetConfig().Kvnodes.Node {
		//GetSugar().Info(v.Stores)

		if err := checkKvnode(v); nil != err {
			panic(err)
		}
		initPd.Kvnodes = append(initPd.Kvnodes, jsonKvnode{
			Id:          v.Id,
			Service:     v.Service,
			RaftService: v.RaftService,
			UdpService:  v.UdpService,
			Stores:      v.Stores,
		})
	}

	for _, v := range stores {
		initPd.Stores = append(initPd.Stores, jsonStore{
			Id:    v.id,
			Slots: v.slots.ToJson(),
		})
	}

	p.issueProposal(&initPdProposal{
		initPd: initPd,
		proposalBase: &proposalBase{
			pd: p,
		},
	})
}

func (p *pd) onBecomeLeader() {

	p.transferingSlot = map[int32]bool{}

	p.tmpTransNodeStore = map[int64]*nodeStoreTransaction{}

	//成为leader,将之前所有处于prepare状态的transfer取消掉
	for _, v := range p.transSlotTransfer {
		if v.TransID > p.nextTransID {
			p.nextTransID = v.TransID
		}

		if v.State == slotTransferPrepare {

			//p.transferingSlot[v.Slot] = true
			for _, vv := range v.Slots {
				p.transferingSlot[vv] = true
			}

			//重发prepare
			prepare := &sproto.SlotTransferPrepare{
				TransID:  v.TransID,
				Slot:     v.Slots,
				StoreIn:  int32(v.InStoreID),
				StoreOut: int32(v.OutStoreID),
			}

			for _, v := range p.stores[v.InStoreID].kvnodes {
				p.udp.SendTo(v.udpAddr, prepare)
			}

			for _, v := range p.stores[v.OutStoreID].kvnodes {
				p.udp.SendTo(v.udpAddr, prepare)
			}

			v.timer = timer.New(time.Second*15, v.onTransTimeout)
		}
	}

	//处理未完成的transNodeStore
	for _, v := range p.transNodeStore {
		if !v.GotLeaderResp || !v.GotOtherResp {
			v.Notify()
		}
	}

	p.nextTransID++

	if len(p.stores) == 0 {
		p.initPd()
	}

}

func (p *pd) Stop() {
	if atomic.CompareAndSwapInt32(&p.stoponce, 0, 1) {
		GetSugar().Info("Stop")
		p.udp.Close()
		p.rn.Stop()
		p.mutilRaft.Stop()
		p.wait.Wait()
	}
}

func (p *pd) processCommited(commited raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply()
		}
	} else {
		err := p.replayProposal(commited.Data)
		if nil != err {
			GetSugar().Panic(err)
		}
	}

	//raft请求snapshot,建立snapshot并返回
	snapshotNotify := commited.GetSnapshotNotify()
	if nil != snapshotNotify {
		snapshot, err := p.getSnapshot()
		if nil != err {
			GetSugar().Panic(err)
		}
		snapshotNotify.Notify(snapshot)
	}
}

func snapMerge(snaps ...[]byte) ([]byte, error) {
	//pd每次都是全量快照，无需合并，返回最后一个即可
	return snaps[len(snaps)-1], nil
}

func (p *pd) serve() {

	go func() {
		defer p.wait.Done()
		for {
			_, v := p.mainque.pop()
			switch v.(type) {
			case error:
				GetSugar().Errorf("error for raft:%v", v.(error))
				return
			case func():
				v.(func())()
			case raft.Committed:
				p.processCommited(v.(raft.Committed))
			case []raft.LinearizableRead:
				//s.processLinearizableRead(v.([]raft.LinearizableRead))
			case raft.ProposalConfChange:
				//s.processConfChange(v.(raft.ProposalConfChange))
			case raft.RemoveFromCluster:
				GetSugar().Info("RemoveFromCluster")
				return
			case raft.ReplayOK:
				p.ready = true
			case raft.RaftStopOK:
				GetSugar().Info("RaftStopOK")
				return
			case raftpb.Snapshot:
				/*snapshot, err := p.loadSnapshot()
				if err != nil {
					GetSugar().Panic(err)
				}
				if snapshot != nil {
					GetSugar().Infof("%x loading snapshot at term %d and index %d", p.rn.ID(), snapshot.Metadata.Term, snapshot.Metadata.Index)
					if err := p.recoverFromSnapshot(snapshot.Data); nil != err {
						GetSugar().Panic(err)
					}
				}*/
			case raft.LeaderChange:
				oldLeader := p.leader
				p.leader = v.(raft.LeaderChange).Leader
				if p.leader == p.raftID {
					p.onBecomeLeader()
				}

				if oldLeader == p.raftID && p.leader != p.raftID {

					for _, v := range p.transNodeStore {
						if nil != v.timer {
							v.timer.Cancel()
							v.timer = nil
						}
					}

					for _, v := range p.transSlotTransfer {
						if nil != v.timer {
							v.timer.Cancel()
							v.timer = nil
						}
					}

					p.tmpTransNodeStore = map[int64]*nodeStoreTransaction{}
				}

			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()

}

func (p *pd) beginSlotTransfer(slot []int32, storeIn *store, storeOut *store) error {

	for _, v := range slot {

		if p.transferingSlot[v] {
			return fmt.Errorf("slot:%d is transfering", v)
		}

		if !storeOut.slots.Test(int(v)) {
			return fmt.Errorf("slot:%d is not found in storeOut:%d", v, storeOut.id)
		}

		if storeIn.slots.Test(int(v)) {
			return fmt.Errorf("slot:%d is already in storeIn:%d", v, storeIn.id)
		}
	}

	trans := &slotTransferTransaction{
		TransID:    p.nextTransID,
		Slots:      slot,
		OutStoreID: storeOut.id,
		InStoreID:  storeIn.id,
		pd:         p,
	}

	p.nextTransID++

	err := p.issueProposal(
		&slotTransferPrepareProposal{
			trans: trans,
			proposalBase: &proposalBase{
				pd: p,
			},
		})

	if nil == err {
		for _, v := range slot {
			p.transferingSlot[v] = true
		}
	}

	return err
}

func (p *pd) getSnapshot() ([]byte, error) {
	snap := jsonSnapShot{}
	for _, v := range p.transSlotTransfer {
		snap.SlotTransactions = append(snap.SlotTransactions, v)
	}

	for _, v := range p.transNodeStore {
		snap.NodeStoreTransactions = append(snap.NodeStoreTransactions, v)
	}

	for _, v := range p.kvnodes {
		j := jsonKvnode{
			Id:          v.id,
			Service:     v.service,
			RaftService: v.raftService,
			UdpService:  v.udpService,
		}

		for k, _ := range v.stores {
			j.Stores = append(j.Stores, k)
		}

		snap.Kvnodes = append(snap.Kvnodes, j)
	}

	for _, v := range p.stores {
		snap.Stores = append(snap.Stores, jsonStore{
			Id:       v.id,
			Slots:    v.slots.ToJson(),
			Removing: v.removing,
		})
	}

	b, err := json.Marshal(snap)

	if nil != err {
		return nil, err
	}

	c := &compress.ZipCompressor{}
	b, err = c.Compress(b)
	return b, err
}

func (p *pd) recoverFromSnapshot(b []byte) error {

	var err error

	unCompressor := &compress.ZipUnCompressor{}

	b, err = unCompressor.UnCompress(b)

	if nil != err {
		return err
	}

	snap := jsonSnapShot{}
	if err = json.Unmarshal(b, &snap); err != nil {
		return err
	}

	p.transSlotTransfer = map[int64]*slotTransferTransaction{}

	for _, v := range snap.SlotTransactions {
		v.pd = p
		p.transSlotTransfer[v.TransID] = v
	}

	p.transNodeStore = map[int64]*nodeStoreTransaction{}

	for _, v := range snap.NodeStoreTransactions {
		v.pd = p
		p.transNodeStore[v.TransID] = v
	}

	p.stores = map[int]*store{}

	p.slot2store = map[int32]*store{}

	for _, v := range snap.Stores {
		s := &store{
			id:       v.Id,
			removing: v.Removing,
			kvnodes:  map[int]*kvnode{},
		}
		var err error
		s.slots, err = bitmap.CreateFromJson(v.Slots)
		if nil != err {
			return err
		}

		slots := s.slots.GetOpenBits()
		for _, vv := range slots {
			p.slot2store[int32(vv)] = s
		}
		p.stores[s.id] = s
	}

	p.kvnodes = map[int]*kvnode{}
	for _, v := range snap.Kvnodes {
		n := &kvnode{
			id:          v.Id,
			service:     v.Service,
			raftService: v.RaftService,
			udpService:  v.UdpService,
			stores:      map[int]*store{},
		}

		n.udpAddr, err = net.ResolveUDPAddr("udp", n.udpService)

		if nil != err {
			return err
		}

		p.kvnodes[v.Id] = n

		for _, vv := range v.Stores {
			s, ok := p.stores[vv]
			if !ok {
				return errors.New("store not found")
			} else {
				s.kvnodes[v.Id] = n
				n.stores[vv] = s
			}
		}
	}

	for _, v := range p.stores {
		v.updateClusterStr()
	}

	return nil
}
