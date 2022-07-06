package client

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type impl struct {
	mu             sync.Mutex
	closed         bool
	waitResp       map[int64]*cmdContext
	waitSend       *list.List //len(avaliableConns)==0时被排队等待发送的请求
	avaliableConns []*conn
	notifyQueue    EventQueueI //响应回调的事件队列
	notifyPriority int         //回调事件优先级
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
	if this.closed {
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
			c.doCallBack(this.notifyQueue, this.notifyPriority, c.getErrorResult(err), nil)
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
				v.doCallBack(this.notifyQueue, this.notifyPriority, v.getErrorResult(err), nil)
			}
		}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
			this.onResponse(msg.(*cs.RespMessage))
		})
		//发送被排队的请求
		for v := conn.waitSend.Front(); v != nil; v = conn.waitSend.Front() {
			cmd := conn.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			this.sendCmd(conn, cmd)
		}
		this.mu.Unlock()
	} else if conn.removed {
		//当前服务已经被移除，尝试通过其它可用服务发送请求
		for v := conn.waitSend.Front(); v != nil; v = conn.waitSend.Front() {
			cmd := conn.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			if avaliableConn := this.getAvaliable(); nil != avaliableConn {
				this.sendCmd(avaliableConn, cmd)
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

func (this *impl) sendCmd(conn *conn, cmd *cmdContext) {
	if nil != conn.session {
		if cmd.req.Timeout = uint32(cmd.deadline.Sub(time.Now()) / time.Millisecond); cmd.req.Timeout > 0 {
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

func (this *impl) sendAgain(cmd *cmdContext) {
	this.mu.Lock()
	if nil != this.waitResp[cmd.req.Seqno] {
		if this.closed {
			delete(this.waitResp, cmd.req.Seqno)
			cmd.stopTimer()
			this.mu.Unlock()
			cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_error, "client closed")), nil)
		} else {
			if avaliableConn := this.getAvaliable(); nil != avaliableConn {
				this.sendCmd(avaliableConn, cmd)
			} else {
				cmd.l = this.waitSend
				cmd.listElement = this.waitSend.PushBack(cmd)
			}
			this.mu.Unlock()
		}
	} else {
		this.mu.Unlock()
	}
}

func (this *impl) onTimeout(cmd *cmdContext) {
	cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errcode.New(errcode.Errcode_timeout, "timeout")), func() {
		this.mu.Lock()
		cmd.stopTimer()
		delete(this.waitResp, cmd.req.Seqno)
		if nil != cmd.listElement {
			cmd.l.Remove(cmd.listElement)
		}
		this.mu.Unlock()
	})
}

func (this *impl) exec(cmd *cmdContext) {
	var errCode errcode.Error
	this.mu.Lock()
	if this.closed {
		errCode = errcode.New(errcode.Errcode_error, "client closed")
	} else if len(this.waitResp) > maxPendingSize {
		errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
	} else {
		this.waitResp[cmd.req.Seqno] = cmd
		cmd.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
		cmd.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, func() { this.onTimeout(cmd) })
		avaliableConn := this.getAvaliable()
		if nil == avaliableConn {
			cmd.l = this.waitSend
			cmd.listElement = this.waitSend.PushBack(cmd)
		} else {
			this.sendCmd(avaliableConn, cmd)
		}
	}
	this.mu.Unlock()
	if nil != errCode {
		cmd.doCallBack(this.notifyQueue, this.notifyPriority, cmd.getErrorResult(errCode), nil)
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
				var ret interface{}
				switch cmd {
				case protocol.CmdType_Get:
					ret = onGetResp(ctx, msg.Err, msg.Data.(*protocol.GetResp))
				case protocol.CmdType_Set:
					ret = onSetResp(ctx, msg.Err, msg.Data.(*protocol.SetResp))
				case protocol.CmdType_SetNx:
					ret = onSetNxResp(ctx, msg.Err, msg.Data.(*protocol.SetNxResp))
				case protocol.CmdType_CompareAndSet:
					ret = onCompareAndSetResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetResp))
				case protocol.CmdType_CompareAndSetNx:
					ret = onCompareAndSetNxResp(ctx, msg.Err, msg.Data.(*protocol.CompareAndSetNxResp))
				case protocol.CmdType_Del:
					ret = onDelResp(ctx, msg.Err, msg.Data.(*protocol.DelResp))
				case protocol.CmdType_IncrBy:
					ret = onIncrByResp(ctx, msg.Err, msg.Data.(*protocol.IncrByResp))
				case protocol.CmdType_Kick:
					ret = onKickResp(ctx, msg.Err, msg.Data.(*protocol.KickResp))
				default:
					ret = ctx.getErrorResult(errcode.New(errcode.Errcode_error, "invaild response"))
				}
				ctx.doCallBack(this.notifyQueue, this.notifyPriority, ret, nil)
			}
		}
	}
}

func (this *impl) onQueryServiceResp(services []string) (bool, time.Duration) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return true, 0
	}

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

	for v := this.waitSend.Front(); v != nil; v = this.waitSend.Front() {
		if avaliableConn := this.getAvaliable(); nil != avaliableConn {
			cmd := this.waitSend.Remove(v).(*cmdContext)
			cmd.listElement = nil
			cmd.l = nil
			this.sendCmd(avaliableConn, cmd)
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
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return
	} else {
		this.closed = true
		for _, v := range this.avaliableConns {
			if nil != v.session {
				v.session.Close(nil, 0)
				v.session = nil
			}
		}
	}
}
