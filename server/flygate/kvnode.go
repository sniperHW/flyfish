package flygate

import (
	"container/list"
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync"
	"time"
)

type kvnode struct {
	sync.Mutex
	waittingSend *list.List //dailing时暂存请求
	waitResponse map[int64]*forwordMsg
	id           int
	service      string
	session      *flynet.Socket
	setID        int
	gate         *gate
}

func (n *kvnode) removeMsg(m *forwordMsg) {
	n.Lock()
	if nil != m.listElement {
		n.waittingSend.Remove(m.listElement)
	}
	m.listElement = nil

	if &n.waitResponse == m.dict {
		delete(n.waitResponse, m.seqno)
		m.dict = nil
	}

	m.clearCache()
	n.Unlock()
}

func (n *kvnode) add2Send(m *forwordMsg) int {
	n.Lock()
	m.listElement = n.waittingSend.PushBack(m)
	m.setCache(n)
	l := n.waittingSend.Len()
	n.Unlock()
	return l
}

func (n *kvnode) add2WaitResp(m *forwordMsg) bool {
	n.Lock()
	if v := n.waitResponse[m.seqno]; nil != v {
		delete(n.waitResponse, m.seqno)
		v.clearCache()
		v.dropReply()
	}
	n.waitResponse[m.seqno] = m
	m.dict = &n.waitResponse
	m.setCache(n)
	n.Unlock()
}

func (n *kvnode) removeWaitResp(seqno int64) (m *forwordMsg) {
	n.Lock()
	m = n.waitResponse[seqno]
	if nil != m {
		delete(n.waitResponse, seqno)
		m.clearCache()
	}
	n.Unlock()
	return
}

func (n *kvnode) waittingSendEmpty() (empty bool) {
	n.Lock()
	empty = n.waittingSend.Len() == 0
	n.Unlock()
	return
}

func (n *kvnode) onConnect(now time.Time) {
	n.Lock()
	for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
		msg := v.Value.(*forwordMsg)
		n.waittingSend.Remove(msg.listElement)
		msg.listElement = nil
		if msg.deadline.After(now) {
			if v := n.waitResponse[msg.seqno]; nil != v {
				delete(n.waitResponse, msg.seqno)
				v.clearCache()
				v.dropReply()
			} else {
				n.send(now, msg)
			}
			n.waitResponse[msg.seqno] = msg
			msg.dict = &n.waitResponse
		} else {
			msg.clearCache()
		}
	}
	n.Unlock()
}

func (n *kvnode) sendForwordMsg(msg *forwordMsg) {
	if nil != n.session {
		now := time.Now()
		if msg.deadline.After(now) && n.add2WaitResp(msg) {
			n.send(now, msg)
		}
	} else if n.add2Send(msg) == 1 {
		n.dial()
	}
}

func (n *kvnode) send(now time.Time, msg *forwordMsg) {
	GetSugar().Debugf("send msg to set:%d kvnode:%d store:%d seqno:%d", n.setID, n.id, int(msg.store&0xFFFFFFFF), msg.seqno)
	binary.BigEndian.PutUint64(msg.bytes[4:], uint64(msg.seqno))
	binary.BigEndian.PutUint32(msg.bytes[4+8:], uint32(msg.store&0xFFFFFFFF))
	binary.BigEndian.PutUint32(msg.bytes[18:], uint32(msg.deadline.Sub(now)/time.Millisecond))
	n.session.Send(msg.bytes)
}

func (n *kvnode) paybackWaittingSendToGate() {
	n.Lock()
	defer n.Unlock()
	for v := n.waittingSend.Front(); nil != v; v = n.waittingSend.Front() {
		msg := v.Value.(*forwordMsg)
		n.waittingSend.Remove(msg.listElement)
		n.gate.addMsg(msg)
	}
}

func (n *kvnode) onResponse(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	if msg := n.removeWaitResp(seqno); nil != msg {
		switch errCode {
		case errcode.Errcode_not_leader:

			//投递到主队列执行
			n.gate.callInQueue(1, func() {
				if s := n.gate.getStore(msg.store); nil != s {
					s.onErrNotLeader(msg)
				} else {
					//store已经被移除，将消息归还到gate.cache
					n.gate.paybackMsg(msg)
				}
			})
		case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering:
			n.gate.callInQueue(1, func() {
				n.gate.paybackMsg(msg)
			})
		default:
			msg.reply(b)
		}
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
			if n.gate.checkKvnode(n) {
				n.session = session
				if nil == err {
					session.SetRecvTimeout(flyproto.PingTime * 10)
					session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
					session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
						n.gate.callInQueue(1, func() {
							n.session = nil
							n.Lock()
							for _, v := range n.waitResponse {
								delete(n.waitResponse, v.seqno)
								v.clearCache()
								v.replyErr(errcode.New(errcode.Errcode_error, "lose connection"))
							}
							n.Unlock()
						})
					}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
						n.onResponse(msg.([]byte))
					})

					n.onConnect(time.Now())

				} else if !n.waittingSendEmpty() {
					n.dial()
				}
			}
		})
	}()
}
