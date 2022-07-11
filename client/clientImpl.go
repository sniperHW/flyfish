package client

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

type impl struct {
	clientImplBase
	avaliableConns []*conn
}

func (this *impl) getAvaliable() *conn {
	if len(this.avaliableConns) > 0 {
		return this.avaliableConns[int(rand.Int31())%len(this.avaliableConns)]
	} else {
		return nil
	}
}

func (this *impl) connect(conn *conn) {
	session, err := cs.NewConnector("tcp", conn.service, outputBufLimit).Dial(time.Second * 1)
	this.mu.Lock()
	if atomic.LoadInt32(&this.closed) == 1 {
		cmds := []*cmdContext{}
		for v := conn.waitSend.Front(); v != nil; v = conn.waitSend.Front() {
			cmd := conn.waitSend.Remove(v).(*cmdContext)
			delete(this.waitResp, cmd.req.Seqno)
			cmd.stopTimer()
			cmds = append(cmds, cmd)
		}
		if nil != session {
			session.Close(nil, 0)
		}
		this.mu.Unlock()
		err := errcode.New(errcode.Errcode_error, "client closed")
		for _, c := range cmds {
			c.doCallBack(this.notifyQueue, this.notifyPriority, c.getErrorResult(err), func() {
				atomic.AddInt64(&this.pendingCount, -1)
			})
		}
	} else if nil == err {
		conn.session = session
		conn.session.SetRecvTimeout(recvTimeout)
		conn.session.SetInBoundProcessor(cs.NewRespInboundProcessor())
		conn.session.SetEncoder(&cs.ReqEncoder{})
		conn.session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
			GetSugar().Infof("socket close %v service:%s", reason, conn.service)
			this.mu.Lock()
			conn.session = nil
			ctxs := []*cmdContext{}
			for _, v := range this.waitResp {
				if v.session == session {
					delete(this.waitResp, v.req.Seqno)
					v.stopTimer()
					ctxs = append(ctxs, v)
				}
			}
			this.mu.Unlock()
			err := errcode.New(errcode.Errcode_error, "lose connection")
			for _, v := range ctxs {
				v.doCallBack(this.notifyQueue, this.notifyPriority, v.getErrorResult(err), func() {
					atomic.AddInt64(&this.pendingCount, -1)
				})
			}
		}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
			this.onResponse(msg.(*cs.RespMessage))
		})
		now := time.Now()
		//发送被排队的请求
		for v := conn.waitSend.Front(); v != nil; v = conn.waitSend.Front() {
			cmd := conn.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			this.sendCmd(conn, cmd, now)
		}
		this.mu.Unlock()
	} else if conn.removed {
		//当前服务已经被移除，尝试通过其它可用服务发送请求
		now := time.Now()
		for v := conn.waitSend.Front(); v != nil; v = conn.waitSend.Front() {
			cmd := conn.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			if avaliableConn := this.getAvaliable(); nil != avaliableConn {
				this.sendCmd(avaliableConn, cmd, now)
			} else {
				cmd.l = this.waitSend
				cmd.listElement = this.waitSend.PushBack(cmd)
			}
		}
		this.mu.Unlock()
	} else {
		this.mu.Unlock()
		time.AfterFunc(1000*time.Millisecond, func() {
			this.connect(conn)
		})
	}
}

func (this *impl) sendCmd(conn *conn, cmd *cmdContext, now time.Time) {
	if nil != conn.session {
		if cmd.req.Timeout = uint32(cmd.deadline.Sub(now) / time.Millisecond); cmd.req.Timeout > 0 {
			cmd.session = conn.session
			conn.session.Send(cmd.req)
		}
	} else {
		cmd.l = conn.waitSend
		cmd.listElement = conn.waitSend.PushBack(cmd)
		if conn.waitSend.Len() == 1 {
			go this.connect(conn)
		}
	}
}

func (this *impl) send(cmd *cmdContext, now time.Time) {
	if avaliableConn := this.getAvaliable(); nil != avaliableConn {
		this.sendCmd(avaliableConn, cmd, now)
	} else {
		cmd.l = this.waitSend
		cmd.listElement = this.waitSend.PushBack(cmd)
	}
}

func (this *impl) onResponse(msg *cs.RespMessage) {
	cmd := protocol.CmdType(msg.Cmd)
	if cmd != protocol.CmdType_Ping {
		var resend bool
		this.mu.Lock()
		ctx, ok := this.waitResp[msg.Seqno]
		if ok {
			if errcode.GetCode(msg.Err) == errcode.Errcode_retry {
				resend = true
			} else {
				delete(this.waitResp, msg.Seqno)
				ctx.stopTimer()
			}
		}
		this.mu.Unlock()
		if ok {
			if resend {
				time.AfterFunc(resendDelay, func() {
					this.sendAgain(ctx)
				})
			} else {
				this.clientImplBase.onResponse(msg, ctx)
			}
		}
	}
}

func (this *impl) onQueryServiceResp(services []string) (bool, time.Duration) {
	if atomic.LoadInt32(&this.closed) == 1 {
		return true, 0
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	localServices := []string{}
	for _, c := range this.avaliableConns {
		localServices = append(localServices, c.service)
	}

	sort.Slice(localServices, func(i, j int) bool {
		return localServices[i] < localServices[j]
	})

	sort.Slice(services, func(i, j int) bool {
		return services[i] < services[j]
	})

	add := []string{}
	remove := []string{}

	i := 0
	j := 0

	for i < len(services) && j < len(localServices) {
		if services[i] == localServices[j] {
			i++
			j++
		} else if services[i] > localServices[j] {
			remove = append(remove, localServices[j])
			j++
		} else {
			add = append(add, services[i])
			i++
		}
	}

	if len(services[i:]) > 0 {
		add = append(add, services[i:]...)
	}

	if len(localServices[j:]) > 0 {
		remove = append(remove, localServices[j:]...)
	}

	for _, v := range add {
		conn := &conn{
			service:  v,
			waitSend: list.New(),
		}
		this.avaliableConns = append(this.avaliableConns, conn)
	}

	for _, v := range remove {
		for i, conn := range this.avaliableConns {
			if conn.service == v {
				this.avaliableConns[i], this.avaliableConns[len(this.avaliableConns)-1] = this.avaliableConns[len(this.avaliableConns)-1], this.avaliableConns[i]
				this.avaliableConns = this.avaliableConns[:len(this.avaliableConns)-1]
				conn.removed = true
				break
			}
		}
	}

	now := time.Now()
	for v := this.waitSend.Front(); v != nil; v = this.waitSend.Front() {
		if avaliableConn := this.getAvaliable(); nil != avaliableConn {
			cmd := this.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			this.sendCmd(avaliableConn, cmd, now)
		} else {
			break
		}
	}

	if len(this.avaliableConns) == 0 {
		return false, time.Millisecond * 100
	} else {
		return false, time.Second * 5
	}
}

func (this *impl) close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.mu.Lock()
		defer this.mu.Unlock()
		this.clientImplBase.close()
		for _, v := range this.avaliableConns {
			if nil != v.session {
				v.session.Close(nil, 0)
				v.session = nil
			}
		}
	}
}
