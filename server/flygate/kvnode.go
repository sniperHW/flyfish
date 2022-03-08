package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type kvnode struct {
	id           int
	service      string
	session      *flynet.Socket
	waittingSend *list.List //dailing时暂存请求
	waitResponse map[int64]*forwordMsg
	config       *Config
	removed      bool
	gate         *gate
}

func (n *kvnode) sendForwordMsg(msg *forwordMsg) bool {

	if len(n.waitResponse)+n.waittingSend.Len() >= n.config.MaxNodePendingMsg {
		return false
	} else {
		if nil != n.session {
			now := time.Now()
			if msg.deadline.After(now) {
				n.send(now, msg)
			} else {
				msg.dropReply()
			}
		} else {
			msg.add(nil, n.waittingSend)
			if n.waittingSend.Len() == 1 {
				n.dial()
			}
		}
		return true
	}
}

func (n *kvnode) send(now time.Time, msg *forwordMsg) {
	GetSugar().Debugf("send msg to set:%d kvnode:%d store:%d seqno:%d nodeSeqno:%d", msg.store.set.setID, n.id, msg.store.id, msg.oriSeqno, msg.seqno)
	binary.BigEndian.PutUint64(msg.bytes[4:], uint64(msg.seqno))
	binary.BigEndian.PutUint32(msg.bytes[4+8:], uint32(msg.store.id))
	binary.BigEndian.PutUint32(msg.bytes[18:], uint32(msg.deadline.Sub(now)/time.Millisecond))
	msg.add(&n.waitResponse, nil)
	n.session.Send(msg.bytes)
}

func (n *kvnode) paybackWaittingSendToGate() {
	for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
		msg := v.Value.(*forwordMsg)
		msg.removeList()
		msg.add(nil, n.gate.pendingMsg)
	}
}

func (n *kvnode) dial() {
	go func() {
		c := cs.NewConnector("tcp", n.service, flynet.OutputBufLimit{
			OutPutLimitSoft:        1024 * 1024 * 10,
			OutPutLimitSoftSeconds: 10,
			OutPutLimitHard:        1024 * 1024 * 50,
		})
		session, err := c.Dial(time.Second * 5)
		n.gate.callInQueue(1, func() {
			if n.removed {
				n.paybackWaittingSendToGate()
				return
			}
			n.session = session
			if nil == err {
				session.SetEncoder(&encoder{})
				session.SetRecvTimeout(flyproto.PingTime * 10)
				session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
				session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
					n.gate.callInQueue(1, func() {
						n.session = nil
						if n.removed {
							n.paybackWaittingSendToGate()
						} else if n.waittingSend.Len() > 0 {
							n.dial()
						}
					})
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					n.gate.callInQueue(1, func() {
						n.onNodeResp(msg.([]byte))
					})
				})

				now := time.Now()
				for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
					msg := v.Value.(*forwordMsg)
					msg.removeList()
					if msg.deadline.After(now) {
						n.send(now, msg)
					} else {
						msg.dropReply()
					}
				}
			} else {
				if n.waittingSend.Len() > 0 {
					n.dial()
				}
			}
		})
	}()
}

func (n *kvnode) onNodeResp(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	GetSugar().Debugf("onNodeResp %d %d", seqno, errCode)
	msg, ok := n.waitResponse[seqno]
	if ok {
		msg.removeMap()
		switch errCode {
		case errcode.Errcode_not_leader:
			msg.store.onErrNotLeader(msg)
		case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering:
			GetSugar().Infof("onForwordError %d oriSeqno:%d slot:%d", errCode, msg.oriSeqno, msg.slot)
			msg.add(nil, n.gate.pendingMsg)
		default:
			//恢复客户端的seqno
			binary.BigEndian.PutUint64(b[4:], uint64(msg.oriSeqno))
			msg.reply(b)
		}
	}
}
