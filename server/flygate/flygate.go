package flygate

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
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"strings"
	"sync"
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
	deadline      time.Time
	bytes         []byte
	deadlineTimer *time.Timer
	store         *store
	node          *node
	gate          *gate
	cli           *flynet.Socket
	replyed       int32
}

func (r *relayMsg) onTimeout() {
	r.node.Lock()
	delete(r.node.pendingReq, r.nodeSeqno)
	r.node.Unlock()
	r.dropReply()
}

func (r *relayMsg) reply(b []byte) {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(&r.gate.pendingMsg, -1)
		r.cli.Send(b)
	}
}

func (r *relayMsg) replyErr(err errcode.Error) {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(&r.gate.pendingMsg, -1)
		replyCliError(r.cli, r.seqno, r.cmd, err)
	}
}

func (r *relayMsg) dropReply() {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(&r.gate.pendingMsg, -1)
	}
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
	config        *Config
	stopOnce      int32
	startOnce     int32
	slotToStore   map[int]*store
	nodes         map[int]*node
	stores        []*store
	listener      *cs.Listener
	pendingMsg    int64
	muC           sync.Mutex
	clients       map[*flynet.Socket]*flynet.Socket
	consoleConn   *flynet.Udp
	muConf        sync.Mutex
	kvconf        *clusterconf.KvConfigJson
	kvconfVersion int
	die           chan struct{}
}

func NewGate(config *Config) *gate {
	return &gate{
		config:      config,
		slotToStore: map[int]*store{},
		nodes:       map[int]*node{},
		clients:     map[*flynet.Socket]*flynet.Socket{},
		die:         make(chan struct{}),
	}
}

func (g *gate) startListener() {
	g.listener.Serve(func(session *flynet.Socket) {
		go func() {

			g.muC.Lock()
			g.clients[session] = session
			g.muC.Unlock()

			session.SetEncoder(&encoder{})
			session.SetSendQueueSize(256)
			session.SetInBoundProcessor(NewCliReqInboundProcessor())

			session.SetCloseCallBack(func(session *flynet.Socket, reason error) {
				g.muC.Lock()
				delete(g.clients, session)
				g.muC.Unlock()
			})

			session.BeginRecv(func(session *flynet.Socket, v interface{}) {
				msg := v.(*relayMsg)
				//g.muRW.RLock()
				s, ok := g.slotToStore[msg.slot]
				//g.muRW.RUnlock()
				if !ok {
					replyCliError(session, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
				} else {
					msg.store = s
					msg.cli = session
					msg.gate = g
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
		g.kvconf, g.kvconfVersion, err = clusterconf.LoadConfigJsonFromDB(clusterConf.ClusterID, clusterConf.DBType, clusterConf.DBHost, clusterConf.DBPort, clusterConf.ConfDB, clusterConf.DBUser, clusterConf.DBPassword)
		if nil != err {
			return err
		}

		for _, v := range g.kvconf.NodeInfo {
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

		if len(g.kvconf.Shard) == 0 {
			return errors.New("shard == 0")
		}

		storeCount := len(g.kvconf.Shard) * clusterconf.StorePerNode

		for i := 0; i < storeCount; i++ {
			g.stores = append(g.stores, &store{
				id:           i + 1,
				gate:         g,
				waittingSend: list.New(),
			})
		}

		for k, v := range g.kvconf.Shard {
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

		if err = g.initConsole(fmt.Sprintf("%s:%d", g.config.ServiceHost, g.config.ConsolePort)); nil != err {
			return err
		}

		g.startListener()

		dirs := strings.Split(g.config.DirService, ";")

		if len(dirs) > 0 {
			go func() {
				for {
					g.muConf.Lock()
					kvconfVersion := g.kvconfVersion
					g.muConf.Unlock()
					for _, v := range dirs {
						if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
							g.consoleConn.SendTo(addr, &sproto.GateReport{
								Service:     fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort),
								Console:     fmt.Sprintf("%s:%d", g.config.ServiceHost, g.config.ConsolePort),
								ConfVersion: int32(kvconfVersion),
							})
						}
					}

					ticker := time.NewTicker((sproto.PingTime - 1) * time.Second)
					select {
					case <-g.die:
						ticker.Stop()
						//服务关闭，通知所有dir立即从gatelist中移除本服务
						for _, v := range dirs {
							if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
								g.consoleConn.SendTo(addr, &sproto.RemoveGate{
									Service: fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort),
								})
							}
						}
						return
					case <-ticker.C:
					}
					ticker.Stop()
				}
			}()
		}
	}

	return nil
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

func (g *gate) Stop() {
	if atomic.CompareAndSwapInt32(&g.stopOnce, 0, 1) {
		close(g.die)

		//首先关闭监听,不在接受新到达的连接
		g.listener.Close()
		//关闭现有连接的读端，不会再接收新的req
		g.muC.Lock()
		for _, v := range g.clients {
			v.ShutdownRead()
		}
		g.muC.Unlock()

		//等待所有消息处理完
		waitCondition(func() bool {
			return atomic.LoadInt64(&g.pendingMsg) == 0
		})

		//关闭现有连接
		g.muC.Lock()
		clients := g.clients
		g.muC.Unlock()

		for _, v := range clients {
			v.Close(nil, time.Second*5)
		}

		waitCondition(func() bool {
			g.muC.Lock()
			defer g.muC.Unlock()
			return len(g.clients) == 0
		})

		g.consoleConn.Close()

	}
}
