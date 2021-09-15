package gate

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/sniperHW/flyfish/server/clusterconf"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type node struct {
	sync.Mutex
	seqCounter   int64
	id           int
	dialing      bool
	service      string
	consoleAddr  *net.UDPAddr
	session      *flynet.Socket
	waittingSend *list.List //dailing时暂存请求
	pendingReq   map[int64]*relayMsg
}

func (n *node) sendRelayReq(req *relayMsg) {
	req.nodeSeqno = atomic.AddInt64(&n.seqCounter, 1)
	//改写seqno
	binary.BigEndian.PutUint64(req.bytes[4:], uint64(req.nodeSeqno))
	//填充storeID
	binary.BigEndian.PutUint32(req.bytes[4+8:], uint32(req.store.id))

	n.Lock()
	if nil != n.session {
		if nil == n.session.Send(req.bytes) {
			n.pendingReq[req.nodeSeqno] = req
			req.deadlineTimer = time.AfterFunc(req.timeout, req.onTimeout)
		} else {
			n.Unlock()
			replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_retry, ""))
			return
		}
	} else {
		n.waittingSend.PushBack(req)
		if !n.dialing {
			n.dial()
		}
	}
	n.Unlock()
}

func (n *node) dial() {
	n.dialing = true
	go func() {
		c := cs.NewConnector("tcp", n.service)
		session, err := c.Dial(time.Second * 5)
		n.Lock()
		n.dialing = false
		n.session = session
		if nil == err {
			//session.SetInBoundProcessor(cs.NewRespInboundProcessor())
			session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
				n.Lock()
				n.session = nil
				pendingReq := n.pendingReq
				n.Unlock()
				for _, v := range pendingReq {
					if v.deadlineTimer.Stop() {
						replyCliError(v.cli, v.seqno, v.cmd, errcode.New(errcode.Errcode_error, "lose connection"))
					}
				}
			}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
				n.onNodeResp(msg.([]byte))
			})

			for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
				req := n.waittingSend.Remove(v).(*relayMsg)
				if nil == session.Send(req.bytes) {
					n.pendingReq[req.nodeSeqno] = req
					req.deadlineTimer = time.AfterFunc(req.timeout, req.onTimeout)
				} else {
					replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_retry, ""))
				}
			}
			n.Unlock()
		} else {
			waittingSend := n.waittingSend
			n.waittingSend = list.New()
			n.Unlock()
			for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
				req := waittingSend.Remove(v).(*relayMsg)
				replyCliError(req.cli, req.seqno, req.cmd, errcode.New(errcode.Errcode_retry, ""))
			}
		}
	}()
}

func (n *node) onNodeResp(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	n.Lock()
	req, ok := n.pendingReq[seqno]
	if ok {
		delete(n.pendingReq, seqno)
		n.Unlock()
		if errCode == errcode.Errcode_not_leader {
			req.store.onLoseLeader(req.version)
		}

		if req.deadlineTimer.Stop() {
			//恢复客户端的seqno
			binary.BigEndian.PutUint64(b[4:], uint64(req.seqno))
			req.cli.Send(b)
		}

	} else {
		n.Unlock()
	}
}

type relayMsg struct {
	nodeSeqno     int64
	version       int64
	slot          int
	seqno         int64
	cmd           uint16
	timeout       time.Duration
	bytes         []byte
	deadlineTimer *time.Timer
	store         *store
	node          *node
	cli           *flynet.Socket
}

func (r *relayMsg) onTimeout() {
	r.node.Lock()
	delete(r.node.pendingReq, r.nodeSeqno)
	r.node.Unlock()
}

func replyCliError(cli *flynet.Socket, seqno int64, cmd uint16, err errcode.Error) {

	var sizeOfErrDesc int

	if nil != err && err.Code != 0 {
		sizeOfErrDesc = len(err.Desc)
		if sizeOfErrDesc > 0xFF {
			//描述超长，直接丢弃
			sizeOfErrDesc = cs.SizeErrDescLen
		} else {
			sizeOfErrDesc += cs.SizeErrDescLen
		}
	}

	payloadLen := cs.SizeSeqNo + cs.SizeCmd + cs.SizeErrCode + sizeOfErrDesc + cs.SizePB
	totalLen := cs.SizeLen + payloadLen
	if uint64(totalLen) > cs.MaxPacketSize {
		return
	}

	b := make([]byte, 0, totalLen)

	//写payload大小
	b = buffer.AppendUint32(b, uint32(payloadLen))
	//seqno
	b = buffer.AppendInt64(b, seqno)
	//cmd
	b = buffer.AppendUint16(b, cmd)
	//err
	b = buffer.AppendInt16(b, errcode.GetCode(err))

	if sizeOfErrDesc > 0 {
		b = buffer.AppendUint16(b, uint16(sizeOfErrDesc-cs.SizeErrDescLen))
		if sizeOfErrDesc > cs.SizeErrDescLen {
			b = buffer.AppendString(b, err.Desc)
		}
	}

	cli.Send(b)
}

func clientReqUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= cs.MinSize {
		payload := int(binary.BigEndian.Uint32(b[r:]))

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+cs.SizeLen > cs.MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+cs.SizeLen)
			return
		}

		totalSize := payload + cs.SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			rr := r + cs.SizeLen
			var m relayMsg
			m.bytes = b[r : r+totalSize]
			m.seqno = int64(binary.BigEndian.Uint64(b[rr:]))
			rr += (8 + 4)
			m.cmd = binary.BigEndian.Uint16(b[rr:])
			rr += 2
			uniKeyLen := int(binary.BigEndian.Uint16(b[rr:]))
			rr += 2
			if uniKeyLen > 0 {
				if uniKeyLen > payload-(rr-r) {
					err = fmt.Errorf("invaild uniKeyLen")
					return
				}
				bb := b[rr : rr+uniKeyLen]
				uniKey := *(*string)(unsafe.Pointer(&bb))
				m.slot = slot.Unikey2Slot(uniKey)
				rr += uniKeyLen
			}
			m.timeout = time.Duration(binary.BigEndian.Uint32(b[rr:]))
			ret = &m
		}
	}
	return
}

type store struct {
	sync.Mutex
	id             int
	queryingLeader bool
	version        int64
	leader         *node
	nodes          []*node
	waittingSend   *list.List //查询leader时的暂存队列
}

func (s *store) onCliMsg(cli *flynet.Socket, msg *relayMsg) {

}

func (s *store) onLoseLeader(version int64) {
	s.Lock()
	if s.version == version && nil != s.leader {
		s.leader = nil
	}
	s.Unlock()
}

type gate struct {
	config      *Config
	stopOnce    int32
	startOnce   int32
	slotToStore map[int]*store
	nodes       map[int]*node
	stores      []*store
}

func (g *gate) onCliMsg(cli *flynet.Socket, msg *relayMsg) {
	s, ok := g.slotToStore[msg.slot]
	if !ok {
		replyCliError(cli, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
	} else {
		s.onCliMsg(cli, msg)
	}
}

func (g *gate) Start() error {
	var err error
	if atomic.CompareAndSwapInt32(&g.startOnce, 0, 1) {
		config := g.config
		if err = os.MkdirAll(config.Log.LogDir, os.ModePerm); nil != err {
			return err
		}

		//从DB获取配置
		clusterConf := config.ClusterConfig
		kvconf, err := clusterconf.LoadConfigJsonFromDB(clusterConf.ClusterID, clusterConf.SqlType, clusterConf.DbHost, clusterConf.DbPort, clusterConf.DbDataBase, clusterConf.DbUser, clusterConf.DbPassword)
		if nil != err {
			return err
		}

		for _, v := range kvconf.NodeInfo {
			n := &node{
				id:      v.ID,
				service: fmt.Sprintf("%s:%d", v.HostIP, v.ServicePort),
			}

			if udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", v.HostIP, v.ConsolePort)); nil != err {
				return err
			} else {
				n.consoleAddr = udpAddr
			}

			g.nodes[v.ID] = n

		}

		storeCount := len(kvconf.Shard) * clusterconf.StorePerNode

		for i := 0; i < storeCount; i++ {
			g.stores = append(g.stores, &store{
				id: i + 1,
			})
		}

		for k, v := range kvconf.Shard {
			for _, vv := range v.Nodes {
				for i := 0; i < clusterconf.StorePerNode; i++ {
					g.stores[k*clusterconf.StorePerNode+i].nodes = append(g.stores[k*clusterconf.StorePerNode+i].nodes, g.nodes[vv])
				}
			}
		}

		jj := 0

		for i := 0; i < slot.SlotCount; i++ {
			jj = (jj + 1) % storeCount
			g.slotToStore[i] = g.stores[jj]
		}

	}

	return nil
}
