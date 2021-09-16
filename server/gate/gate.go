package gate

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/server/clusterconf"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"sync/atomic"
	"time"
)

type encoder struct {
}

func (this *encoder) EnCode(o interface{}, buff *buffer.Buffer) error {
	b, ok := o.([]byte)
	if !ok {
		return errors.New("invaild o")
	}

	buff.AppendBytes(b)

	return nil
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

	b = buffer.AppendInt32(b, 0)

	cli.Send(b)
}

type gate struct {
	config      *Config
	stopOnce    int32
	startOnce   int32
	slotToStore map[int]*store
	nodes       map[int]*node
	stores      []*store
	listener    *cs.Listener
}

func NewGate(config *Config) *gate {
	return &gate{
		config:      config,
		slotToStore: map[int]*store{},
		nodes:       map[int]*node{},
	}
}

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		go func() {
			session.SetEncoder(&encoder{})
			session.SetSendQueueSize(256)
			session.SetInBoundProcessor(NewCliReqInboundProcessor())

			//session.SetCloseCallBack(func(session *net.Socket, reason error) {
			//})

			session.BeginRecv(func(session *flynet.Socket, v interface{}) {
				msg := v.(*relayMsg)
				s, ok := g.slotToStore[msg.slot]
				if !ok {
					replyCliError(session, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
				} else {
					msg.store = s
					msg.cli = session
					s.onCliMsg(session, msg)
				}

			})
		}()
	})
}

func verifyLogin(loginReq *flyproto.LoginReq) bool {
	return true
}

func (g *gate) Start() error {
	var err error
	if atomic.CompareAndSwapInt32(&g.startOnce, 0, 1) {
		config := g.config
		if err = os.MkdirAll(config.Log.LogDir, os.ModePerm); nil != err {
			return err
		}

		g.listener, err = cs.NewListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort), verifyLogin)

		if nil != err {
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
				id:           v.ID,
				service:      fmt.Sprintf("%s:%d", v.HostIP, v.ServicePort),
				gate:         g,
				waittingSend: list.New(),
				pendingReq:   map[int64]*relayMsg{},
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
				id:           i + 1,
				gate:         g,
				waittingSend: list.New(),
			})
		}

		for k, v := range kvconf.Shard {
			for _, vv := range v.Nodes {
				for i := 0; i < clusterconf.StorePerNode; i++ {
					g.stores[k*clusterconf.StorePerNode+i].nodes = append(g.stores[k*clusterconf.StorePerNode+i].nodes, g.nodes[vv])
				}
			}
		}

		/*for _, v := range g.stores {
			fmt.Println("store", v.id)
			for _, vv := range v.nodes {
				fmt.Println(vv.id, vv.service)
			}
		}*/

		jj := 0

		for i := 0; i < slot.SlotCount; i++ {
			jj = (jj + 1) % storeCount
			g.slotToStore[i] = g.stores[jj]
		}

		g.startListener()

	}

	return nil
}
