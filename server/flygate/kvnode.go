package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
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
	set          *set
	mainQueue    *queue.PriorityQueue
	config       *Config
	removed      bool
	gate         *gate
}

func (n *kvnode) sendForwordMsg(msg *forwordMsg) bool {
	//改写seqno
	binary.BigEndian.PutUint64(msg.bytes[4:], uint64(msg.seqno))
	//填充storeID
	binary.BigEndian.PutUint32(msg.bytes[4+8:], uint32(msg.store.id))

	if nil != n.session {
		if len(n.waitResponse) >= n.config.MaxNodePendingMsg {
			return false
		} else {
			GetSugar().Debugf("send msg to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, msg.store.id, msg.oriSeqno, msg.seqno)
			binary.BigEndian.PutUint32(msg.bytes[18:], uint32(timeout/time.Millisecond))
			msg.add(&n.waitResponse, nil)
			session.Send(msg.bytes)
			return true
		}
	} else {
		if n.waittingSend.Len() >= n.config.MaxNodePendingMsg {
			return false
		} else {
			msg.add(nil, n.waittingSend)
			if len(n.waittingSend) == 1 {
				n.dial()
			}
		}
	}
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
		n.mainQueue.ForceAppend(1, func() {
			if n.set.removed || n.removed {
				n.paybackWaittingSendToGate()
				return
			}
			n.session = session
			if nil == err {
				session.SetEncoder(&encoder{})
				session.SetRecvTimeout(flyproto.PingTime * 10)
				session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
				session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
					n.mainQueue.ForceAppend(1, func() {
						n.session = nil
						if n.set.removed || n.removed {
							n.paybackWaittingSendToGate()
						} else if len(n.waittingSend) > 0 {
							n.dial()
						}
					})
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					n.mainQueue.ForceAppend(0, func() {
						n.onNodeResp(msg.([]byte))
					})
				})
				for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
					msg := v.Value.(*forwordMsg)
					msg.removeList()
					GetSugar().Debugf("send msg to kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.id, msg.store.id, msg.oriSeqno, msg.seqno)
					binary.BigEndian.PutUint32(msg.bytes[18:], uint32(timeout/time.Millisecond))
					msg.add(&n.waitResponse, nil)
					session.Send(msg.bytes)
				}
			} else {
				if len(n.waittingSend) > 0 {
					n.dial()
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
		msg.removeMap()
		switch errCode {
		case errcode.Errcode_not_leader:
			msg.store.onErrNotLeader(msg)
		case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering:
			GetSugar().Infof("onForwordError %d oriSeqno:%d slot:%d", errCode, msg.oriSeqno, msg.slot)
			msg.replyErr(errcode.New(errcode.Errcode_retry, "please retry later"))
		default:
			//恢复客户端的seqno
			binary.BigEndian.PutUint64(b[4:], uint64(msg.oriSeqno))
			msg.reply(b)
		}
	}
}
