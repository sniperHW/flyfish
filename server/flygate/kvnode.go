package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"net"
	"sync"
	"sync/atomic"
	"time"
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
	gate         *gate
}

func (n *node) sendRelayReq(req *relayMsg) {
	now := time.Now()
	if req.deadline.After(now) {
		req.node = n
		req.nodeSeqno = atomic.AddInt64(&n.seqCounter, 1)
		//改写seqno
		binary.BigEndian.PutUint64(req.bytes[4:], uint64(req.nodeSeqno))
		//填充storeID
		binary.BigEndian.PutUint32(req.bytes[4+8:], uint32(req.store.id))

		n.Lock()
		if nil != n.session {
			if len(n.pendingReq) >= n.gate.config.MaxNodePendingMsg {
				n.Unlock()
				req.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
				return
			} else {
				GetSugar().Infof("send req to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, req.store.id, req.seqno, req.nodeSeqno)
				timeout := req.deadline.Sub(now)
				if timeout > time.Millisecond {
					binary.BigEndian.PutUint32(req.bytes[18:], uint32(timeout/time.Millisecond))
					if nil == n.session.Send(req.bytes) {
						req.bytes = nil
						n.pendingReq[req.nodeSeqno] = req
						req.deadlineTimer = time.AfterFunc(timeout, req.onTimeout)
					} else {
						n.Unlock()
						req.replyErr(errcode.New(errcode.Errcode_retry, ""))
						return
					}
				} else {
					req.dropReply()
				}
			}
		} else {
			if n.waittingSend.Len() >= n.gate.config.MaxNodePendingMsg {
				n.Unlock()
				req.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
				return
			} else {
				n.waittingSend.PushBack(req)
				if !n.dialing {
					n.dial()
				}
			}
		}
		n.Unlock()
	} else {
		req.dropReply()
	}
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
			session.SetEncoder(&encoder{})
			session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
			session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
				n.Lock()
				n.session = nil
				pendingReq := n.pendingReq
				n.Unlock()
				for _, v := range pendingReq {
					if v.deadlineTimer.Stop() {
						v.dropReply()
					}
				}
			}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
				n.onNodeResp(msg.([]byte))
			})

			for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
				req := n.waittingSend.Remove(v).(*relayMsg)
				timeout := req.deadline.Sub(time.Now())
				if timeout > time.Millisecond {
					GetSugar().Infof("send req to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, req.store.id, req.seqno, req.nodeSeqno)
					binary.BigEndian.PutUint32(req.bytes[18:], uint32(timeout/time.Millisecond))
					if nil == session.Send(req.bytes) {
						req.bytes = nil
						n.pendingReq[req.nodeSeqno] = req
						req.deadlineTimer = time.AfterFunc(timeout, req.onTimeout)
					} else {
						req.replyErr(errcode.New(errcode.Errcode_retry, ""))
					}
				} else {
					req.dropReply()
				}
			}
			n.Unlock()
		} else {
			waittingSend := n.waittingSend
			n.waittingSend = list.New()
			n.Unlock()
			for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
				req := waittingSend.Remove(v).(*relayMsg)
				req.replyErr(errcode.New(errcode.Errcode_retry, ""))
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
		if req.deadlineTimer.Stop() {
			if errCode == errcode.Errcode_not_leader {
				req.replyErr(errcode.New(errcode.Errcode_retry, ""))
				req.store.onLoseLeader(req.version)
			} else {
				//恢复客户端的seqno
				binary.BigEndian.PutUint64(b[4:], uint64(req.seqno))
				req.reply(b)
			}
		}
	} else {
		n.Unlock()
	}
}
