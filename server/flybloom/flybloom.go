package flybloom

import (
	//"container/list"
	//"encoding/json"
	"errors"
	//"github.com/gogo/protobuf/proto"
	//"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/server/flybloom/bloomfilter"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type replyer interface {
	reply(*snet.Message)
}

type udpReplyer struct {
	replyed int32
	from    *net.UDPAddr
	fb      *flybloom
}

func (ur *udpReplyer) reply(resp *snet.Message) {
	if atomic.CompareAndSwapInt32(&ur.replyed, 0, 1) {
		ur.fb.udp.SendTo(ur.from, resp)
	}
}

type tcpReplyer struct {
	replyed int32
	from    *flynet.Socket
}

func (tr *tcpReplyer) reply(resp *snet.Message) {
	if atomic.CompareAndSwapInt32(&tr.replyed, 0, 1) {
		tr.from.Send(resp)
	}
}

type applicationQueue struct {
	q *queue.PriorityQueue
}

type flybloom struct {
	leader    uint64
	ready     bool
	rn        *raft.RaftInstance
	cluster   int
	mutilRaft *raft.MutilRaft
	mainque   applicationQueue
	closed    int32
	wait      sync.WaitGroup
	config    *Config
	service   string
	RaftIDGen *idutil.Generator
	udp       *flynet.Udp
	filter    *bloomfilter.Filter
	pdAddr    []*net.UDPAddr
	listener  *net.TCPListener
}

var outputBufLimit flynet.OutputBufLimit = flynet.OutputBufLimit{
	OutPutLimitSoft:        cs.MaxPacketSize,
	OutPutLimitSoftSeconds: 10,
	OutPutLimitHard:        cs.MaxPacketSize * 10,
}

func NewFlyBloom(nodeID uint16, cluster int, join bool, config *Config, clusterStr string) (*flybloom, error) {

	peers, err := raft.SplitPeers(clusterStr)

	if nil != err {
		return nil, err
	}

	self, ok := peers[nodeID]

	if !ok {
		return nil, errors.New("cluster not contain self")
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, 10000),
	}

	filter, err := bloomfilter.NewOptimal(config.MaxElements, config.ProbCollide)

	if nil != err {
		return nil, err
	}

	fb := &flybloom{
		mainque:   mainQueue,
		config:    config,
		service:   self.ClientURL,
		cluster:   cluster,
		RaftIDGen: idutil.NewGenerator(nodeID, time.Now()),
		filter:    filter,
	}

	pd := strings.Split(config.PD, ";")

	for _, v := range pd {
		addr, err := net.ResolveUDPAddr("udp", v)
		if nil != err {
			return nil, err
		} else {
			fb.pdAddr = append(fb.pdAddr, addr)
		}
	}

	fb.mutilRaft = raft.NewMutilRaft()

	fb.rn, err = raft.NewInstance(nodeID, cluster, join, fb.mutilRaft, fb.mainque, peers, fb.config.RaftLogDir, fb.config.RaftLogPrefix)

	if nil != err {
		return nil, err
	}

	if err = fb.startUdpService(); nil != err {
		fb.rn.Stop()
		return nil, err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", fb.service)
	if err != nil {
		fb.rn.Stop()
		return nil, err
	}

	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fb.rn.Stop()
		return nil, err
	}

	fb.listener = l

	go func() {
		for {
			conn, err := fb.listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				} else {
					return
				}

			} else {
				go fb.onClient(flynet.NewSocket(conn, outputBufLimit))
			}
		}
	}()

	GetSugar().Infof("mutilRaft serve on:%s", self.URL)

	go fb.mutilRaft.Serve([]string{self.URL})

	fb.wait.Add(1)
	go fb.serve()

	return fb, nil
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	q.q.ForceAppend(1, m)
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

func (fb *flybloom) onClient(session *flynet.Socket) {
	session.SetRecvTimeout(time.Second * 10)
	//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
	session.SetInBoundProcessor(snet.NewReqInboundProcessor())
	session.SetEncoder(&snet.Encoder{})
	session.BeginRecv(func(session *flynet.Socket, v interface{}) {
		if atomic.LoadInt32(&fb.closed) == 1 {
			return
		}
		fb.onMsg(&tcpReplyer{
			from: session,
		}, v.(*snet.Message))
	})
}

func (fb *flybloom) onMsg(replyer replyer, msg *snet.Message) {
	switch msg.Msg.(type) {
	case *sproto.BloomContainKeyReq:
		hash := fb.filter.HashString(msg.Msg.(*sproto.BloomContainKeyReq).Key)
		fb.mainque.append(func() {
			replyer.reply(&snet.Message{
				Context: msg.Context,
				Msg: &sproto.BloomContainKeyResp{
					Contain: fb.filter.ContainsWithHashs(hash),
				},
			})
		})
	case *sproto.BloomAddKey:
		hash := fb.filter.HashString(msg.Msg.(*sproto.BloomContainKeyReq).Key)
		fb.mainque.append(func() {
			if fb.ready && !fb.filter.ContainsWithHashs(hash) {
				fb.rn.IssueProposal(&ProposalAdd{
					Hash: hash,
				})
			}
		})
	}
}

func (fb *flybloom) startUdpService() error {
	udp, err := flynet.NewUdp(fb.service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	GetSugar().Infof("flybloom start udp at %s", fb.service)

	fb.udp = udp

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := udp.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				fb.onMsg(&udpReplyer{from: from, fb: fb}, msg.(*snet.Message))
			}
		}
	}()
	return nil
}

func (fb *flybloom) getSnapshot() ([]byte, error) {
	return fb.filter.MarshalBinaryZip()
}

func (fb *flybloom) recoverFromSnapshot(b []byte) error {
	return fb.filter.UnmarshalBinaryZip(b)
}

func (fb *flybloom) processCommited(commited raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply(fb)
		}
	} else {
		err := fb.replayProposal(commited.Data)
		if nil != err {
			GetSugar().Panic(err)
		}
	}

	//raft请求snapshot,建立snapshot并返回
	snapshotNotify := commited.GetSnapshotNotify()
	if nil != snapshotNotify {
		snapshot, err := fb.getSnapshot()
		if nil != err {
			GetSugar().Panic(err)
		}
		snapshotNotify.Notify(snapshot)
	}
}

func (fb *flybloom) serve() {

	go func() {
		defer func() {
			fb.udp.Close()
			fb.mutilRaft.Stop()
			fb.mainque.close()
			fb.wait.Done()
			GetSugar().Infof("pd serve break")
		}()
		for {
			_, v := fb.mainque.pop()
			switch v.(type) {
			case raft.TransportError:
				GetSugar().Errorf("error for raft transport:%v", v.(raft.TransportError))
			case func():
				v.(func())()
			case raft.Committed:
				fb.processCommited(v.(raft.Committed))
			case []raft.LinearizableRead:
			case raft.ProposalConfChange:
				//v.(*ProposalConfChange).reply(nil)
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode && c.NodeID == fb.rn.ID() {
					GetSugar().Infof("%x Remove from cluster", fb.rn.ID())
					fb.rn.Stop()
				}
			case raft.ReplayOK:
			case raft.RaftStopOK:
				GetSugar().Info("RaftStopOK")
				return
			case raftpb.Snapshot:
				snapshot := v.(raftpb.Snapshot)
				GetSugar().Infof("%x loading snapshot at term %d and index %d", fb.rn.ID(), snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := fb.recoverFromSnapshot(snapshot.Data); err != nil {
					GetSugar().Panic(err)
				}
			case raft.LeaderChange:
				oldLeader := fb.leader
				atomic.StoreUint64(&fb.leader, v.(raft.LeaderChange).Leader)
				if fb.leader == fb.rn.ID() {
					//fb.onBecomeLeader()
				}

				if oldLeader == fb.rn.ID() && fb.leader != fb.rn.ID() {
					//p.onLeaderDownToFollower()
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()
}
