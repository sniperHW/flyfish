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

type cacheKvnode struct {
	sync.Mutex
	waittingSend *list.List //dailing时暂存请求
	waitResponse map[int64]*forwordMsg
}

func (ck *cacheKvnode) remove(m *forwordMsg) {
	ck.Lock()
	if nil != m.listElement {
		ck.waittingSend.Remove(m.listElement)
	}
	m.listElement = nil

	if &ck.waitResponse == m.dict {
		delete(ck.waitResponse, m.seqno)
		m.dict = nil
	}

	m.clearCache()
	ck.Unlock()
}

func (ck *cacheKvnode) add2Send(m *forwordMsg) int {
	ck.Lock()
	m.listElement = ck.waittingSend.PushBack(m)
	m.setCache(ck)
	l := ck.waittingSend.Len()
	ck.Unlock()
	return l
}

func (ck *cacheKvnode) add2WaitResp(m *forwordMsg) {
	ck.Lock()
	ck.waitResponse[m.seqno] = m
	m.dict = &ck.waitResponse
	m.setCache(ck)
	ck.Unlock()
}

func (ck *cacheKvnode) removeWaitResp(seqno int64) (m *forwordMsg) {
	ck.Lock()
	m = ck.waitResponse[seqno]
	if nil != m {
		delete(ck.waitResponse, seqno)
		m.clearCache()
	}
	ck.Unlock()
	return
}

func (ck *cacheKvnode) dropAllWaitResp() {
	GetSugar().Infof("-----------dropAllWaitResp---------------------")
	ck.Lock()
	for _, v := range ck.waitResponse {
		delete(ck.waitResponse, v.seqno)
		v.clearCache()
		v.deadlineTimer.stop()
	}
	ck.Unlock()
}

func (ck *cacheKvnode) waittingSendEmpty() (empty bool) {
	ck.Lock()
	empty = ck.waittingSend.Len() == 0
	ck.Unlock()
	return
}

type kvnode struct {
	id      int
	service string
	session *flynet.Socket
	cache   cacheKvnode
	setID   int
	gate    *gate
}

func (n *kvnode) sendForwordMsg(msg *forwordMsg) {
	if nil != n.session {
		now := time.Now()
		if msg.deadline.After(now) {
			n.cache.add2WaitResp(msg)
			n.send(now, msg)
		}
	} else if n.cache.add2Send(msg) == 1 {
		n.dial()
	}
}

func (n *kvnode) send(now time.Time, msg *forwordMsg) {
	GetSugar().Debugf("send msg to set:%d kvnode:%d store:%d seqno:%d nodeSeqno:%d", n.setID, n.id, int(msg.store&0xFFFFFFFF), msg.oriSeqno, msg.seqno)
	binary.BigEndian.PutUint64(msg.bytes[4:], uint64(msg.seqno))
	binary.BigEndian.PutUint32(msg.bytes[4+8:], uint32(msg.store&0xFFFFFFFF))
	binary.BigEndian.PutUint32(msg.bytes[18:], uint32(msg.deadline.Sub(now)/time.Millisecond))
	n.session.Send(msg.bytes)
}

func (n *kvnode) paybackWaittingSendToGate() {
	n.cache.Lock()
	for v := n.cache.waittingSend.Front(); nil != v; v = n.cache.waittingSend.Front() {
		msg := v.Value.(*forwordMsg)
		msg.listElement = nil
		msg.clearCache()
		msg.dropReply()
	}
	n.cache.Unlock()
}

func (n *kvnode) onResponse(b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	if msg := n.cache.removeWaitResp(seqno); nil != msg {
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
			//恢复客户端的seqno
			binary.BigEndian.PutUint64(b[4:], uint64(msg.oriSeqno))
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
			if !n.gate.checkKvnode(n) {
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
						n.cache.dropAllWaitResp()
						for _, v := range n.gate.sets {
							for _, vv := range v.stores {
								if vv.leader == n {
									vv.leader = nil
								}
							}
						}
					})
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					n.onResponse(msg.([]byte))
				})

				now := time.Now()

				n.cache.Lock()
				for v := n.cache.waittingSend.Front(); nil != v; v = n.cache.waittingSend.Front() {
					msg := v.Value.(*forwordMsg)
					n.cache.waittingSend.Remove(msg.listElement)
					msg.listElement = nil
					if msg.deadline.After(now) {
						n.cache.waitResponse[msg.seqno] = msg
						msg.dict = &n.cache.waitResponse
						msg.setCache(&n.cache)
						n.send(now, msg)
					} else {
						msg.clearCache()
					}
				}
				n.cache.Unlock()
			} else if !n.cache.waittingSendEmpty() {
				n.dial()
			}
		})
	}()
}
