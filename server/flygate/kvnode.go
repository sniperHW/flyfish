package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"net"
	"time"
)

type kvnode struct {
	id           int
	dialing      bool
	service      string
	udpAddr      *net.UDPAddr
	session      *flynet.Socket
	waittingSend *list.List //dailing时暂存请求
	pendingReq   map[int64]*relayMsg
	set          *set
	mainQueue    *queue.PriorityQueue
	config       *Config
	removed      bool
}

func (n *kvnode) sendRelayMsg(req *relayMsg) {
	now := time.Now()
	if req.deadline.After(now) {
		//改写seqno
		binary.BigEndian.PutUint64(req.bytes[4:], uint64(req.seqno))
		//填充storeID
		binary.BigEndian.PutUint32(req.bytes[4+8:], uint32(req.store.id))

		if nil != n.session {
			if len(n.pendingReq) >= n.config.MaxNodePendingMsg {
				req.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
			} else {
				GetSugar().Infof("send req to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, req.store.id, req.oriSeqno, req.seqno)
				timeout := req.deadline.Sub(now)
				if timeout > time.Millisecond {
					binary.BigEndian.PutUint32(req.bytes[18:], uint32(timeout/time.Millisecond))
					if nil == n.session.Send(req.bytes) {
						req.bytes = nil
						n.pendingReq[req.seqno] = req
						req.pendingReq = &n.pendingReq
						req.deadlineTimer = time.AfterFunc(timeout, req.onTimeout)
					} else {
						req.replyErr(errcode.New(errcode.Errcode_retry, ""))
						return
					}
				} else {
					req.dropReply()
				}
			}
		} else {
			if n.waittingSend.Len() >= n.config.MaxNodePendingMsg {
				req.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
				return
			} else {
				req.l = n.waittingSend
				req.listElement = n.waittingSend.PushBack(req)
				if !n.dialing {
					n.dial()
				}
			}
		}
	} else {
		req.dropReply()
	}
}

func (n *kvnode) dial() {
	n.dialing = true
	go func() {
		c := cs.NewConnector("tcp", n.service, flynet.OutputBufLimit{
			OutPutLimitSoft:        1024 * 1024 * 10,
			OutPutLimitSoftSeconds: 10,
			OutPutLimitHard:        1024 * 1024 * 50,
		})
		session, err := c.Dial(time.Second * 5)
		n.mainQueue.ForceAppend(1, func() {
			if n.set.removed || n.removed {
				return
			}
			n.dialing = false
			n.session = session
			if nil == err {
				session.SetEncoder(&encoder{})
				session.SetRecvTimeout(flyproto.PingTime * 10)
				session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
				session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
					n.mainQueue.ForceAppend(1, func() {
						n.session = nil
						for _, v := range n.pendingReq {
							if v.deadlineTimer.Stop() {
								v.dropReply()
							}
						}
					})
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					n.mainQueue.ForceAppend(0, func() {
						n.onNodeResp(msg.([]byte))
					})
				})

				for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
					req := n.waittingSend.Remove(v).(*relayMsg)
					req.l = nil
					req.listElement = nil
					timeout := req.deadline.Sub(time.Now())
					if timeout > time.Millisecond {
						GetSugar().Infof("send req to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, req.store.id, req.oriSeqno, req.seqno)
						binary.BigEndian.PutUint32(req.bytes[18:], uint32(timeout/time.Millisecond))
						if nil == session.Send(req.bytes) {
							req.bytes = nil
							n.pendingReq[req.seqno] = req
							req.pendingReq = &n.pendingReq
							req.deadlineTimer = time.AfterFunc(timeout, req.onTimeout)
						} else {
							req.replyErr(errcode.New(errcode.Errcode_retry, ""))
						}
					} else {
						req.dropReply()
					}
				}
			} else {
				waittingSend := n.waittingSend
				n.waittingSend = list.New()
				for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
					req := waittingSend.Remove(v).(*relayMsg)
					req.l = nil
					req.listElement = nil
					req.replyErr(errcode.New(errcode.Errcode_retry, ""))
				}
			}
		})
	}()
}

func (n *kvnode) onNodeResp(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	req, ok := n.pendingReq[seqno]
	if ok {
		delete(n.pendingReq, seqno)
		req.pendingReq = nil
		if req.deadlineTimer.Stop() {
			if errCode == errcode.Errcode_not_leader {
				req.store.onErrNotLeader(req)
			} else {
				//恢复客户端的seqno
				binary.BigEndian.PutUint64(b[4:], uint64(req.oriSeqno))
				req.reply(b)
			}
		}
	}
}
