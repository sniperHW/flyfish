package raft

//go test -covermode=count -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"reflect"
	//"strconv"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"go.uber.org/zap"
	//"strings"
	"errors"
	"sync"
	"testing"
	"time"
)

type testStorage struct {
	storage []byte
}

func (s *testStorage) SaveMemberShip(_ *zap.Logger, data []byte) error {
	s.storage = data
	return nil
}

func (s *testStorage) LoadMemberShip(lg *zap.Logger) (*membership.MemberShip, error) {
	if len(s.storage) == 0 {
		return nil, errors.New("key not found")
	} else {
		return membership.NewMemberShipFromJson(lg, s.storage)
	}
}

func (s *testStorage) IsKeyNotFound(err error) bool {
	if "key not found" == err.Error() {
		return true
	} else {
		return false
	}
}

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

func (this TestConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this TestConfChange) OnError(err error) {
	this.ch <- err
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

func (s *kvstore) Get(key string) (string, error) {
	o := &operationGet{
		key: key,
		ch:  make(chan []interface{}, 1),
	}
	err := s.mainQueue.AppendOp(o)
	if nil != err {
		fmt.Println(err)
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
		fmt.Println(err)
	}
	r := <-o.ch
	if nil == r {
		return nil
	} else {
		return r.(error)
	}
}

func (s *kvstore) AddMember(id uint64, url string) error {

	if err := s.rn.MayAddMember(membership.Member{
		ID:       types.ID(id),
		PeerURLs: []string{url},
	}); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeAddNode,
		url:            url,
		nodeID:         id,
		ch:             make(chan error, 1),
	}

	if err := s.rn.IssueConfChange(o); nil != err {
		return err
	}

	return <-o.ch
}

func (s *kvstore) AddLearner(id uint64, url string) error {

	if err := s.rn.MayAddMember(membership.Member{
		ID:        types.ID(id),
		PeerURLs:  []string{url},
		IsLearner: true,
	}); nil != err {
		return err
	}

	o := TestConfChange{
		confChangeType: raftpb.ConfChangeAddLearnerNode,
		url:            url,
		nodeID:         id,
		ch:             make(chan error, 1),
	}

	if err := s.rn.IssueConfChange(o); nil != err {
		return err
	}

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

	if err := s.rn.IssueConfChange(o); nil != err {
		return err
	}

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

	if err := s.rn.IssueConfChange(o); nil != err {
		return err
	}

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
				//fmt.Println("set", v.Key, "=", v.Val)
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
				GetSugar().Infof("-----------------ConfChange %v %d---------------------", v.(ConfChange).CCType, v.(ConfChange).NodeID)
			case ReplayOK:
				if nil != s.startOK {
					s.startOK()
				}
			case raftpb.Snapshot:
				snapshot := v.(raftpb.Snapshot)
				GetSugar().Infof("%x loading snapshot at term %d and index %d", s.rn.id, snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			case LeaderChange:
				if v.(LeaderChange).Leader == int(s.rn.ID()) {
					if nil != s.becomeLeader {
						s.becomeLeader()
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

func newKvNode(nodeID uint16, shard uint16, mb *membership.MemberShip) *kvnode {

	var selfUrl []string

	id := MakeInstanceID(nodeID, shard)

	if m := mb.Member(types.ID(id)); nil != m {
		selfUrl = m.PeerURLs
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2),
	}

	mutilRaft := NewMutilRaft()

	rn, err := NewInstance(nodeID, shard, mutilRaft, mainQueue, mb, "log", "kv")

	if nil != err {
		fmt.Println(err)
	}

	store := newKVStore(mainQueue, rn)

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
	os.RemoveAll("./log/kv-1-1-wal")
	os.RemoveAll("./log/kv-1-1-snap")
	os.RemoveAll("./log/kv-2-1-wal")
	os.RemoveAll("./log/kv-2-1-snap")
	os.RemoveAll("./log/kv-3-1-wal")
	os.RemoveAll("./log/kv-3-1-snap")
	os.RemoveAll("./log/kv-4-1-wal")
	os.RemoveAll("./log/kv-4-1-snap")

	ProposalFlushInterval = 10
	ProposalBatchCount = 1
	ReadFlushInterval = 10
	ReadBatchCount = 1
	DefaultSnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	u, _ := types.NewURLs([]string{"http://127.0.0.1:12379"})

	membs := []*membership.Member{
		membership.NewMember(types.ID(MakeInstanceID(1, 1)), u),
	}

	st := &testStorage{}
	mb := membership.NewMemberShipMembers(GetLogger(), types.ID(1), types.ID(1), membs)
	mb.SetStorage(st)

	fmt.Println(mb.Members())

	{

		node := newKvNode(1, 1, mb)

		startOkCh := make(chan struct{})

		becomeLeaderCh := make(chan struct{})

		node.store.startOK = func() {
			startOkCh <- struct{}{}
		}

		node.store.becomeLeader = func() {
			becomeLeaderCh <- struct{}{}
		}

		<-startOkCh

		<-becomeLeaderCh

		node.store.Set("sniperHW", "ok")
		assert.Equal(t, "ok", node.store.kvStore["sniperHW"])

		for i := 0; i < 500; i++ {
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
		//start again
		node := newKvNode(1, 1, mb)

		startOkCh := make(chan struct{})

		becomeLeaderCh := make(chan struct{})

		node.store.startOK = func() {
			startOkCh <- struct{}{}
		}

		node.store.becomeLeader = func() {
			becomeLeaderCh <- struct{}{}
		}

		<-startOkCh

		<-becomeLeaderCh

		node.stop()
	}
}

func TestCluster(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/kv-1-1-wal")
	os.RemoveAll("./log/kv-1-1-snap")
	os.RemoveAll("./log/kv-2-1-wal")
	os.RemoveAll("./log/kv-2-1-snap")
	os.RemoveAll("./log/kv-3-1-wal")
	os.RemoveAll("./log/kv-3-1-snap")
	os.RemoveAll("./log/kv-4-1-wal")
	os.RemoveAll("./log/kv-4-1-snap")

	ProposalFlushInterval = 10
	ProposalBatchCount = 1
	ReadFlushInterval = 10
	ReadBatchCount = 1
	DefaultSnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	u1, _ := types.NewURLs([]string{"http://127.0.0.1:22378"})
	u2, _ := types.NewURLs([]string{"http://127.0.0.1:22379"})
	u3, _ := types.NewURLs([]string{"http://127.0.0.1:22380"})
	u4, _ := types.NewURLs([]string{"http://127.0.0.1:22381"})

	membs1 := []*membership.Member{
		membership.NewMember(types.ID(MakeInstanceID(1, 1)), u1),
		membership.NewMember(types.ID(MakeInstanceID(2, 1)), u2),
		membership.NewMember(types.ID(MakeInstanceID(3, 1)), u3),
	}

	st1 := &testStorage{}
	mb1 := membership.NewMemberShipMembers(GetLogger(), types.ID(1), types.ID(1), membs1)
	mb1.SetStorage(st1)

	node1 := newKvNode(1, 1, mb1)

	becomeLeaderCh1 := make(chan *kvnode, 1)

	node1.store.becomeLeader = func() {
		becomeLeaderCh1 <- node1
	}

	st2 := &testStorage{}
	mb2 := membership.NewMemberShipMembers(GetLogger(), types.ID(2), types.ID(1), membs1)
	mb2.SetStorage(st2)

	node2 := newKvNode(2, 1, mb2)

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2.store.becomeLeader = func() {
		becomeLeaderCh2 <- node2
	}

	st3 := &testStorage{}
	mb3 := membership.NewMemberShipMembers(GetLogger(), types.ID(3), types.ID(1), membs1)
	mb3.SetStorage(st3)

	node3 := newKvNode(3, 1, mb3)

	becomeLeaderCh3 := make(chan *kvnode, 1)

	node3.store.becomeLeader = func() {
		becomeLeaderCh3 <- node3
	}

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

	for i := 0; i < 500; i++ {
		leader.store.Set(fmt.Sprintf("sniperHW:%d", i), fmt.Sprintf("sniperHW:%d", i))
	}

	leader.store.Set("sniperHW", "sniperHW")
	r, _ := leader.store.Get("sniperHW")
	assert.Equal(t, r, "sniperHW")

	time.Sleep(time.Second)

	//加入新节点
	newNodeID := uint64((4 << 16) + 1)
	err := leader.store.AddLearner(newNodeID, "http://127.0.0.1:22381")
	assert.Nil(t, err)

	fmt.Println("AddLearner ok")

	membs2 := []*membership.Member{
		membership.NewMember(types.ID(MakeInstanceID(1, 1)), u1),
		membership.NewMember(types.ID(MakeInstanceID(2, 1)), u2),
		membership.NewMember(types.ID(MakeInstanceID(3, 1)), u3),
		membership.NewMember(types.ID(MakeInstanceID(4, 1)), u4),
	}

	st4 := &testStorage{}
	mb4 := membership.NewMemberShipMembers(GetLogger(), types.ID(4), types.ID(1), membs2)
	mb4.SetStorage(st4)

	node4 := newKvNode(4, 1, mb4)

	startOkCh4 := make(chan struct{}, 1)

	node4.store.startOK = func() {
		select {
		case startOkCh4 <- struct{}{}:
		default:
		}
	}

	<-startOkCh4

	GetSugar().Info("startOkCh4")

	for nil != leader.rn.IsLearnerReady(newNodeID) {
		fmt.Println("wait for learner ready")
		time.Sleep(time.Second)
	}

	err = leader.store.PromoteLearner(newNodeID)
	assert.Nil(t, err)

	for {
		_, ok := node4.store.kvStore["sniperHW"]
		if ok {
			assert.Equal(t, "sniperHW", node4.store.kvStore["sniperHW"])
			break
		}
	}

	//test remove node
	leader.store.RemoveMember(uint64((4 << 16) + 1))

	node4.stop()

	time.Sleep(time.Second * 5)

	node1.stop()

	node2.stop()

	node3.stop()

}

func TestProposeTimeout(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./log/kv-1-1-wal")
	os.RemoveAll("./log/kv-1-1-snap")
	os.RemoveAll("./log/kv-2-1-wal")
	os.RemoveAll("./log/kv-2-1-snap")
	os.RemoveAll("./log/kv-3-1-wal")
	os.RemoveAll("./log/kv-3-1-snap")
	os.RemoveAll("./log/kv-4-1-wal")
	os.RemoveAll("./log/kv-4-1-snap")

	ProposalFlushInterval = 10
	ProposalBatchCount = 1
	ReadFlushInterval = 10
	ReadBatchCount = 1
	DefaultSnapshotCount = 100
	SnapshotCatchUpEntriesN = 100

	u1, _ := types.NewURLs([]string{"http://127.0.0.1:32378"})
	u2, _ := types.NewURLs([]string{"http://127.0.0.1:32379"})

	membs1 := []*membership.Member{
		membership.NewMember(types.ID(MakeInstanceID(1, 1)), u1),
		membership.NewMember(types.ID(MakeInstanceID(2, 1)), u2),
	}

	st1 := &testStorage{}
	mb1 := membership.NewMemberShipMembers(GetLogger(), types.ID(1), types.ID(1), membs1)
	mb1.SetStorage(st1)

	node1 := newKvNode(1, 1, mb1)

	becomeLeaderCh1 := make(chan *kvnode, 1)

	node1.store.becomeLeader = func() {
		becomeLeaderCh1 <- node1
	}

	st2 := &testStorage{}
	mb2 := membership.NewMemberShipMembers(GetLogger(), types.ID(1), types.ID(2), membs1)
	mb2.SetStorage(st2)

	node2 := newKvNode(2, 1, mb2)

	becomeLeaderCh2 := make(chan *kvnode, 1)

	node2.store.becomeLeader = func() {
		becomeLeaderCh2 <- node2
	}

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

	go func() {
		/*
		 *  只剩下一个节点,propose无法提交
		 *  当另一个节点恢复后，最终将成功提交
		 */
		fmt.Println("set", leader.store.Set("sniperHW", "ok"))
	}()

	if node1 == nil {
		node1 = newKvNode(1, 1, mb1)
	}

	if node2 == nil {
		node2 = newKvNode(2, 1, mb2)
	}

	time.Sleep(time.Second * 5)

}
