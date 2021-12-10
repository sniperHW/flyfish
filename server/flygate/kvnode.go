package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"time"
)

type kvnode struct {
	id           int
	dialing      bool
	service      string
	session      *flynet.Socket
	waittingSend *list.List //dailing时暂存请求
	waitResponse map[int64]*forwordMsg
	set          *set
	mainQueue    *queue.PriorityQueue
	config       *Config
	removed      bool
	gate         *gate
}

func (n *kvnode) sendForwordMsg(msg *forwordMsg) {
	now := time.Now()
	if msg.deadline.After(now) {
		//改写seqno
		binary.BigEndian.PutUint64(msg.bytes[4:], uint64(msg.seqno))
		//填充storeID
		binary.BigEndian.PutUint32(msg.bytes[4+8:], uint32(msg.store.id))

		if nil != n.session {
			if len(n.waitResponse) >= n.config.MaxNodePendingMsg {
				msg.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
			} else {
				GetSugar().Debugf("send msg to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, msg.store.id, msg.oriSeqno, msg.seqno)
				timeout := msg.deadline.Sub(now)
				if timeout > time.Millisecond {
					binary.BigEndian.PutUint32(msg.bytes[18:], uint32(timeout/time.Millisecond))
					if nil == n.session.Send(msg.bytes) {
						msg.bytes = nil
						n.waitResponse[msg.seqno] = msg
						msg.waitResponse = &n.waitResponse
						msg.deadlineTimer = time.AfterFunc(timeout, msg.onTimeout)
					} else {
						msg.replyErr(errcode.New(errcode.Errcode_retry, ""))
						return
					}
				} else {
					msg.dropReply()
				}
			}
		} else {
			if n.waittingSend.Len() >= n.config.MaxNodePendingMsg {
				msg.replyErr(errcode.New(errcode.Errcode_gate_busy, ""))
				return
			} else {
				msg.l = n.waittingSend
				msg.listElement = n.waittingSend.PushBack(msg)
				if !n.dialing {
					n.dial()
				}
			}
		}
	} else {
		msg.dropReply()
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
						for _, v := range n.waitResponse {
							v.deadlineTimer.Stop()
							v.deadlineTimer = nil
							v.dropReply()
						}
						n.waitResponse = map[int64]*forwordMsg{}

					})
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					n.mainQueue.ForceAppend(0, func() {
						n.onNodeResp(msg.([]byte))
					})
				})

				for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
					msg := n.waittingSend.Remove(v).(*forwordMsg)
					msg.l = nil
					msg.listElement = nil
					timeout := msg.deadline.Sub(time.Now())
					if timeout > time.Millisecond {
						GetSugar().Debugf("send msg to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, msg.store.id, msg.oriSeqno, msg.seqno)
						binary.BigEndian.PutUint32(msg.bytes[18:], uint32(timeout/time.Millisecond))
						if nil == session.Send(msg.bytes) {
							msg.bytes = nil
							n.waitResponse[msg.seqno] = msg
							msg.waitResponse = &n.waitResponse
							msg.deadlineTimer = time.AfterFunc(timeout, msg.onTimeout)
						} else {
							msg.replyErr(errcode.New(errcode.Errcode_retry, ""))
						}
					} else {
						msg.dropReply()
					}
				}
			} else {
				waittingSend := n.waittingSend
				n.waittingSend = list.New()
				for v := waittingSend.Front(); nil != v; v = waittingSend.Front() {
					msg := waittingSend.Remove(v).(*forwordMsg)
					msg.l = nil
					msg.listElement = nil
					msg.replyErr(errcode.New(errcode.Errcode_retry, ""))
				}
			}
		})
	}()
}

func (n *kvnode) onNodeResp(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	msg, ok := n.waitResponse[seqno]
	if ok {
		delete(n.waitResponse, seqno)
		msg.waitResponse = nil
		switch errCode {
		case errcode.Errcode_not_leader:
			msg.store.onErrNotLeader(msg)
		case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering:
			n.mainQueue.ForceAppend(1, func() {
				n.gate.onForwordError(errCode, msg)
			})
		default:
			msg.deadlineTimer.Stop()
			msg.deadlineTimer = nil
			//恢复客户端的seqno
			binary.BigEndian.PutUint64(b[4:], uint64(msg.oriSeqno))
			msg.reply(b)
		}
	}
}
