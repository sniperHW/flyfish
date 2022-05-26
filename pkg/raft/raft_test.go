package raft

//go test -race -covermode=atomic -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"encoding/json"
	//"errors"
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

var RaftIDGen *idutil.Generator = idutil.NewGenerator(1, time.Now())

func init() {
	pprof := flag.String("pprof", "localhost:8899", "pprof")
	go func() {
		http.ListenAndServe(*pprof, nil)
	}()
}

type applicationQueue struct {
	q *queue.PriorityQueue
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	if err := q.q.ForceAppend(1, m); nil != err {
		panic(err)
	}
}

func (q applicationQueue) AppendOp(op interface{}) error {
	return q.q.ForceAppend(0, op)
}

func (q applicationQueue) Pop() (closed bool, v interface{}) {
	return q.q.Pop()
}

func (q applicationQueue) Close() {
	q.q.Close()
}

type operationSet struct {
	v  kv
	ch chan error
}

type operationGet struct {
	key string
	ch  chan []interface{}
}

type KVProposal struct {
	v  kv
	ch chan error
}

func (p *KVProposal) Isurgent() bool {
	return false
}

func (p *KVProposal) OnError(err error) {
	p.ch <- err
}

func (p *KVProposal) Serilize(b []byte) []byte {
	bytes, _ := json.Marshal(p.v)
	b = buffer.AppendUint32(b, uint32(len(bytes)))
	return buffer.AppendBytes(b, bytes)
}

//不做任何进一步的处理
func (p *KVProposal) OnMergeFinish(b []byte) []byte {
	return b
}

type KVLinearizableRead struct {
	key string
	ch  chan []interface{}
}

func (p *KVLinearizableRead) OnError(err error) {
	p.ch <- []interface{}{"", err}
}

type TestConfChange struct {
	confChangeType raftpb.ConfChangeType
	isPromote      bool
	url            string //for add
	nodeID         uint64
	ch             chan error
	processID      uint16
}

func (this TestConfChange) IsPromote() bool {
	return this.isPromote
}

func (this TestConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this TestConfChange) GetURL() string {
	return this.url
}

func (this TestConfChange) GetClientURL() string {
	return this.url
}

func (this TestConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this TestConfChange) OnError(err error) {
	this.ch <- err
}

func (this TestConfChange) GetProcessID() uint16 {
	return this.processID
}

type kvstore struct {
	mu           sync.RWMutex
	mainQueue    applicationQueue
	kvStore      map[string]string // current committed key-value pairs
	rn           *RaftInstance
	becomeLeader func()
	startOK      func()
	//onApplySnapOK func()
	raftStopCh chan struct{}
}

type kv struct {
	Key string
	Val string
}

func newKVStore(mainQueue applicationQueue, rn *RaftInstance) *kvstore {
	s := &kvstore{
		mainQueue:  mainQueue,
		kvStore:    make(map[string]string),
		rn:         rn,
		raftStopCh: make(chan struct{}),
	}

	go s.serve()

	return s
}

func (s *kvstore) setStartOK(fn func()) {
	s.mu.Lock()
	s.startOK = fn
	s.mu.Unlock()
}

func (s *kvstore) setBecomeLeader(fn func()) {
	s.mu.Lock()
	s.becomeLeader = fn
	s.mu.Unlock()
}

func (s *kvstore) Get(key string) (string, error) {
	o := &operationGet{
		key: key,
		ch:  make(chan []interface{}, 1),
	}
	err := s.mainQueue.AppendOp(o)

	if nil != err {
		return "", err
	}

	r := <-o.ch
	if r[1] == nil {
		return r[0].(string), nil
	} else {
		return "", r[1].(error)
	}
}

func (s *kvstore) Set(key string, val string) error {
	o := &operationSet{
		v: kv{
			Key: key,
			Val: val,
		},
		ch: make(chan error, 1),
	}
	err := s.mainQueue.AppendOp(o)
	if nil != err {
		return err
	}
	r := <-o.ch
	if nil == r {
		return nil
	} else {
		return r.(error)
	}
}

func (s *kvstore) SetDirectly(key string, val string) error {

	ch := make(chan error, 1)

	p := &KVProposal{
		v: kv{
			Key: key,
			Val: val,
		},
		ch: ch,
	}

	s.rn.propose([]Proposal{p})

	r := <-ch
	if nil == r {
		return nil
	} else {
		return r.(error)
	}

}

func (s *kvstore) AddMember(processID uint16, id uint64, url string) error {

	if err := s.rn.MayAddMember(types.ID(id)); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeAddNode,
		url:            url,
		nodeID:         id,
		processID:      processID,
		ch:             make(chan error, 1),
	}

	s.rn.IssueConfChange(o)
	return <-o.ch
}

func (s *kvstore) AddLearner(processID uint16, id uint64, url string) error {

	if err := s.rn.MayAddMember(types.ID(id)); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeAddLearnerNode,
		url:            url,
		nodeID:         id,
		processID:      processID,
		ch:             make(chan error, 1),
	}

	s.rn.IssueConfChange(o)
	return <-o.ch
}

func (s *kvstore) PromoteLearner(id uint64) error {
	if err := s.rn.IsLearnerReady(id); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeAddNode,
		nodeID:         id,
		ch:             make(chan error, 1),
		isPromote:      true,
	}

	s.rn.IssueConfChange(o)

	return <-o.ch

}

func (s *kvstore) RemoveMember(id uint64) error {
	if err := s.rn.MayRemoveMember(types.ID(id)); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeRemoveNode,
		nodeID:         id,
		ch:             make(chan error, 1),
	}

	s.rn.IssueConfChange(o)
	return <-o.ch
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bb, err := json.Marshal(s.kvStore)
	if nil != err {
		return nil, err
	} else {
		b := buffer.New()
		b.AppendUint32(uint32(len(bb)))
		b.AppendBytes(bb)
		return b.Bytes(), nil
	}
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	store := map[string]string{}
	r := buffer.NewReader(snapshot)
	for !r.IsOver() {
		l := r.GetUint32()
		b := r.GetBytes(int(l))
		var s map[string]string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}

		for k, vv := range s {
			store[k] = vv
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}

func (s *kvstore) processOperation(o interface{}) {
	switch o.(type) {
	case *operationGet:
		s.rn.IssueLinearizableRead(&KVLinearizableRead{
			key: o.(*operationGet).key,
			ch:  o.(*operationGet).ch,
		})
	case *operationSet:
		s.rn.IssueProposal(&KVProposal{
			v:  o.(*operationSet).v,
			ch: o.(*operationSet).ch,
		})
	default:
		panic("here")
	}
}

func (s *kvstore) processConfChange(p ProposalConfChange) {
	p.(TestConfChange).ch <- nil
}

func (s *kvstore) processCommited(commited *Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			o := v.(*KVProposal)
			s.kvStore[o.v.Key] = o.v.Val
			o.ch <- nil
		}
	} else {
		r := buffer.NewReader(commited.Data)
		for {
			if l := r.GetUint32(); l == 0 {
				break
			} else {
				bytes := r.GetBytes(int(l))
				var v kv
				if err := json.Unmarshal(bytes, &v); err != nil {
					break
				}
				s.kvStore[v.Key] = v.Val
			}
		}
	}

	//raft请求snapshot,建立snapshot并返回
	snapshotNotify := commited.GetSnapshotNotify()
	if nil != snapshotNotify {
		bytes, _ := s.getSnapshot()
		snapshotNotify.Notify(bytes)
	}
}

func (s *kvstore) processLinearizableRead(r []LinearizableRead) {
	for _, v := range r {
		rr := v.(*KVLinearizableRead)
		rr.ch <- []interface{}{s.kvStore[rr.key], nil}
	}
}

func (s *kvstore) serve() {
	for {
		closed, v := s.mainQueue.Pop()
		if closed {
			return
		} else {
			switch v.(type) {
			case TransportError:
				GetSugar().Infof("(raft error) %x %v", s.rn.id, v.(TransportError))
			case *operationGet, *operationSet:
				s.processOperation(v)
			case Committed:
				c := v.(Committed)
				s.processCommited(&c)
			case []LinearizableRead:
				s.processLinearizableRead(v.([]LinearizableRead))
			case ProposalConfChange:
				s.processConfChange(v.(ProposalConfChange))
			case ConfChange:
			case ReplayOK:
				GetSugar().Infof("replay ok")
				var startOK func()
				s.mu.Lock()
				startOK = s.startOK
				s.mu.Unlock()
				if nil != startOK {
					startOK()
				}
			case raftpb.Snapshot:
				snapshot := v.(raftpb.Snapshot)
				GetSugar().Infof("%x loading snapshot at term %d and index %d", s.rn.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			case LeaderChange:
				if v.(LeaderChange).Leader == s.rn.ID() {

					var becomeLeader func()
					s.mu.Lock()
					becomeLeader = s.becomeLeader
					s.mu.Unlock()
					if nil != becomeLeader {
						becomeLeader()
					}
				}
			case RaftStopOK:
				GetSugar().Infof("got RaftStopOK----------------")
				close(s.raftStopCh)
			default:
				fmt.Println("here", v, reflect.TypeOf(v).String())
			}
		}
	}
}

func init() {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", "info", 100, 14, 10, true))
}

type kvnode struct {
	mutilRaft *MutilRaft
	rn        *RaftInstance
	store     *kvstore
}

var (
	SnapshotCount           uint64
	SnapshotCatchUpEntriesN uint64
)

func newKvNode(nodeid uint16, cid int, join bool, cluster string, startok func(), becomeLeader func()) *kvnode {

	peers, err := SplitPeers(cluster)

	if nil != err {
		panic(err)
	}

	selfUrl := peers[nodeid].URL

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2),
	}

	mutilRaft := NewMutilRaft()

	rn, err := NewInstance(nodeid, cid, join, mutilRaft, mainQueue, peers, RaftInstanceOption{
		Logdir:                  "./log/raftLog",
		RaftLogPrefix:           "kv",
		SnapshotCount:           SnapshotCount,
		SnapshotCatchUpEntriesN: SnapshotCatchUpEntriesN,
	})

	if nil != err {
		fmt.Println(err)
	}

	store := newKVStore(mainQueue, rn)

	store.setStartOK(startok)

	store.setBecomeLeader(becomeLeader)

	go mutilRaft.Serve(selfUrl)

	return &kvnode{
		mutilRaft: mutilRaft,
		rn:        rn,
		store:     store,
	}

}

func (this *kvnode) stop() {
	this.rn.Stop()
	<-this.store.raftStopCh
	this.store.mainQueue.Close()
	this.mutilRaft.Stop()
}
func TestSingleNode(t *testing.T) {

	//先删除所有kv文件
	os.RemoveAll("./log/raftLog")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	raftID := RaftIDGen.Next()
	raftCluster := fmt.Sprintf("1@%d@http://127.0.0.1:12379@http://127.0.0.1:12379@", raftID)

	{

		startOkCh := make(chan struct{})

		becomeLeaderCh := make(chan struct{})

		node := newKvNode(1, 1, false, raftCluster, func() {
			startOkCh <- struct{}{}
		}, func() {
			becomeLeaderCh <- struct{}{}
		})

		<-startOkCh

		<-becomeLeaderCh

		node.store.Set("sniperHW", "ok")
		assert.Equal(t, "ok", node.store.kvStore["sniperHW"])

		for i := 0; i < 10; i++ {
			node.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
		}

		node.store.Set("sniperHW", "sniperHW")
		r, _ := node.store.Get("sniperHW")
		assert.Equal(t, r, "sniperHW")

		time.Sleep(time.Second)

		node.stop()

	}

	fmt.Println("start again")

	{
		startOkCh := make(chan struct{})

		becomeLeaderCh := make(chan struct{})

		//start again
		node := newKvNode(1, 1, false, raftCluster, func() {
			startOkCh <- struct{}{}
		}, func() {
			becomeLeaderCh <- struct{}{}
		})

		<-startOkCh

		<-becomeLeaderCh

		err := node.store.AddLearner(2, 2, "http://127.0.0.1:22381")
		assert.Nil(t, err)

		err = node.store.AddLearner(2, 2, "http://127.0.0.1:22381")
		assert.Equal(t, membership.ErrIDExists, err)

		for i := 0; i < 500; i++ {
			node.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
		}

		node.stop()
	}

	{

		//start again
		startOkCh := make(chan struct{})

		becomeLeaderCh := make(chan struct{})

		node := newKvNode(1, 1, false, raftCluster, func() {
			startOkCh <- struct{}{}
		}, func() {
			becomeLeaderCh <- struct{}{}
		})

		<-startOkCh

		<-becomeLeaderCh

		node.stop()
	}

}

func TestCluster(t *testing.T) {
	//先删除所有kv文件
	//os.RemoveAll("./log/raftLog")
	os.RemoveAll("./log")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	raftID1 := RaftIDGen.Next()
	raftID2 := RaftIDGen.Next()
	raftID3 := RaftIDGen.Next()
	raftID4 := RaftIDGen.Next()

	cluster := fmt.Sprintf("1@%d@http://127.0.0.1:22378@http://127.0.0.1:22378@,2@%d@http://127.0.0.1:22379@http://127.0.0.1:22379@,3@%d@http://127.0.0.1:22380@http://127.0.0.1:22380@",
		raftID1, raftID2, raftID3)

	becomeLeaderCh1 := make(chan *kvnode, 1)

	var node1 *kvnode
	var node2 *kvnode
	var node3 *kvnode

	var mu sync.Mutex

	mu.Lock()

	node1 = newKvNode(1, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh1 <- node1
	})

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2 = newKvNode(2, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh2 <- node2
	})

	becomeLeaderCh3 := make(chan *kvnode, 1)

	node3 = newKvNode(3, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh3 <- node3
	})

	mu.Unlock()

	getLeader := func() *kvnode {
		select {
		case n := <-becomeLeaderCh1:
			return n
		case n := <-becomeLeaderCh2:
			return n
		case n := <-becomeLeaderCh3:
			return n
		}
	}

	leader := getLeader()

	for i := 0; i < 100; i++ {
		leader.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
	}

	leader.store.Set("sniperHW", "sniperHW")
	r, _ := leader.store.Get("sniperHW")
	assert.Equal(t, r, "sniperHW")

	//加入新节点
	//newNodeID := MakeInstanceID(4, 1, 5).Uint64()

	var err error

	for {
		err = leader.store.AddLearner(4, raftID4, "http://127.0.0.1:22381")
		if nil == err {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	fmt.Println("AddLearner ok")

	cluster = cluster + fmt.Sprintf(",4@%d@http://127.0.0.1:22381@http://127.0.0.1:22381@learner", raftID4)

	startOkCh4 := make(chan struct{}, 1)

	node4 := newKvNode(4, 1, true, cluster, func() {
		select {
		case startOkCh4 <- struct{}{}:
		default:
		}
	}, nil)

	<-startOkCh4

	GetSugar().Info("startOkCh4")

	for nil != leader.rn.IsLearnerReady(raftID4) {
		fmt.Println("wait for learner ready")
		time.Sleep(time.Second)
	}

	err = leader.store.PromoteLearner(raftID4)
	assert.Nil(t, err)

	for {
		_, ok := node4.store.kvStore["sniperHW"]
		if ok {
			assert.Equal(t, "sniperHW", node4.store.kvStore["sniperHW"])
			break
		}
	}

	GetSugar().Infof("%v", leader.rn.Members())

	//test remove node
	leader.store.RemoveMember(raftID4)

	GetSugar().Infof("%v", leader.rn.Members())

	node4.stop()

	time.Sleep(time.Second * 5)

	node1.stop()

	node2.stop()

	node3.stop()

}

func TestDownToFollower(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/raftLog")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	raftID1 := RaftIDGen.Next()
	raftID2 := RaftIDGen.Next()

	cluster := fmt.Sprintf("1@%d@http://127.0.0.1:22378@http://127.0.0.1:22378@,2@%d@http://127.0.0.1:22379@http://127.0.0.1:22379@",
		raftID1, raftID2)

	var node1 *kvnode
	var node2 *kvnode

	var mu sync.Mutex

	mu.Lock()

	becomeLeaderCh1 := make(chan *kvnode, 1)

	node1 = newKvNode(1, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh1 <- node1
	})

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2 = newKvNode(2, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh2 <- node2
	})

	mu.Unlock()

	getLeader := func() *kvnode {
		select {
		case n := <-becomeLeaderCh1:
			fmt.Println("node1 becomeLeader")
			return n
		case n := <-becomeLeaderCh2:
			fmt.Println("node2 becomeLeader")
			return n
		}
	}

	leader := getLeader()

	if node1 != leader {
		node1.stop()
		node1 = nil
	}

	if node2 != leader {
		node2.stop()
		node2 = nil
	}

	ret := leader.store.SetDirectly("sniperHW", "ok")

	fmt.Println("set result", ret)

	if node1 != nil {
		node1.stop()
	}

	if node2 != nil {
		node2.stop()
	}
}

func TestOneNodeDownAndRestart(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/raftLog")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100
	checkQuorum = false

	raftID1 := RaftIDGen.Next()
	raftID2 := RaftIDGen.Next()

	cluster := fmt.Sprintf("1@%d@http://127.0.0.1:22378@http://127.0.0.1:22378@,2@%d@http://127.0.0.1:22379@http://127.0.0.1:22379@",
		raftID1, raftID2)

	var node1 *kvnode
	var node2 *kvnode

	var mu sync.Mutex

	mu.Lock()

	becomeLeaderCh1 := make(chan *kvnode, 1)

	node1 = newKvNode(1, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh1 <- node1
	})

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2 = newKvNode(2, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh2 <- node2
	})

	mu.Unlock()

	getLeader := func() *kvnode {
		select {
		case n := <-becomeLeaderCh1:
			fmt.Println("node1 becomeLeader")
			return n
		case n := <-becomeLeaderCh2:
			fmt.Println("node2 becomeLeader")
			return n
		}
	}

	leader := getLeader()

	if node1 != leader {
		node1.stop()
		node1 = nil
	}

	if node2 != leader {
		node2.stop()
		node2 = nil
	}

	ch := make(chan struct{})

	//在另一个节点重启完成后成功
	go func() {
		begin := time.Now()
		ret := leader.store.Set("sniperHW", "ok")
		fmt.Println("set result", ret, "use", time.Now().Sub(begin))
		close(ch)
	}()

	time.Sleep(time.Second)
	if node1 == nil {
		node1 = newKvNode(1, 1, false, cluster, nil, nil)
	}

	if node2 == nil {
		node2 = newKvNode(2, 1, false, cluster, nil, nil)
	}

	<-ch

	if node1 != nil {
		node1.stop()
	}

	if node2 != nil {
		node2.stop()
	}

	checkQuorum = true

}

func TestTransferLeader(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/raftLog")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	raftID1 := RaftIDGen.Next()
	raftID2 := RaftIDGen.Next()

	cluster := fmt.Sprintf("1@%d@http://127.0.0.1:22378@http://127.0.0.1:22378@,2@%d@http://127.0.0.1:22379@http://127.0.0.1:22379@",
		raftID1, raftID2)

	becomeLeaderCh1 := make(chan *kvnode, 1)

	var mu sync.Mutex

	var node1 *kvnode
	var node2 *kvnode

	mu.Lock()

	node1 = newKvNode(1, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh1 <- node1
	})

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2 = newKvNode(2, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh2 <- node2
	})

	mu.Unlock()

	getLeader := func() *kvnode {
		select {
		case n := <-becomeLeaderCh1:
			fmt.Println("node1 becomeLeader")
			return n
		case n := <-becomeLeaderCh2:
			fmt.Println("node2 becomeLeader")
			return n
		}
	}

	leader := getLeader()

	for i := 0; i < 10; i++ {
		leader.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
	}

	var follower *kvnode
	if leader == node1 {
		follower = node2
	} else {
		follower = node1
	}

	for follower.rn.Lead() != uint64(follower.rn.id) {
		err := leader.rn.TransferLeadership(uint64(follower.rn.id))
		fmt.Println(err)
	}

	node1.stop()
	node2.stop()
}

func TestFollower(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/raftLog")

	SnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	raftID1 := RaftIDGen.Next()
	raftID2 := RaftIDGen.Next()

	cluster := fmt.Sprintf("1@%d@http://127.0.0.1:22378@http://127.0.0.1:22378@,2@%d@http://127.0.0.1:22379@http://127.0.0.1:22379@",
		raftID1, raftID2)

	becomeLeaderCh1 := make(chan *kvnode, 1)

	var node1 *kvnode
	var node2 *kvnode

	var mu sync.Mutex

	mu.Lock()

	node1 = newKvNode(1, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh1 <- node1
	})

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2 = newKvNode(2, 1, false, cluster, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		becomeLeaderCh2 <- node2
	})

	mu.Unlock()

	getLeader := func() *kvnode {
		select {
		case n := <-becomeLeaderCh1:
			fmt.Println("node1 becomeLeader")
			return n
		case n := <-becomeLeaderCh2:
			fmt.Println("node2 becomeLeader")
			return n
		}
	}

	leader := getLeader()

	var follower *kvnode
	if leader == node1 {
		follower = node2
	} else {
		follower = node1
	}

	for i := 0; i < 10; i++ {
		fmt.Println(follower.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i)))
	}

	for i := 0; i < 10; i++ {
		leader.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
	}

	leader.stop()
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		fmt.Println(follower.store.Get(fmt.Sprintf("sniperHW:%d", i)))
	}

	follower.stop()
}
