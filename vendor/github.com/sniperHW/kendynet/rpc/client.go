package rpc

import (
	"container/list"
	"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
)

var ErrCallTimeout error = fmt.Errorf("rpc call timeout")
var ErrChannelDisconnected error = fmt.Errorf("channel disconnected")
var sequence uint64
var client_once sync.Once
var timerMgrs []*timer.TimerMgr

type RPCResponseHandler func(interface{}, error)

type reqContext struct {
	seq         uint64
	onResponse  RPCResponseHandler
	listEle     *list.Element
	channelName string
	c           *RPCClient
}

type channelReqContexts struct {
	reqs *list.List
}

func (this *channelReqContexts) add(req *reqContext) {
	req.listEle = this.reqs.PushBack(req)
}

func (this *channelReqContexts) remove(req *reqContext) bool {
	if nil != req.listEle {
		this.reqs.Remove(req.listEle)
		req.listEle = nil
		return true
	} else {
		return false
	}
}

func (this *channelReqContexts) onChannelDisconnect() {
	for {
		if v := this.reqs.Front(); nil != v {
			mgr := timerMgrs[v.Value.(*reqContext).seq%uint64(len(timerMgrs))]
			if ok, _ := mgr.CancelByIndex(v.Value.(*reqContext).seq); ok {
				v.Value.(*reqContext).onResponse(nil, ErrChannelDisconnected)
			}
		} else {
			break
		}
	}
}

func (this *reqContext) onTimeout(_ *timer.Timer, _ interface{}) {
	kendynet.GetLogger().Infoln("req timeout", this.seq, time.Now())
	if this.c.removeChannelReq(this) {
		this.onResponse(nil, ErrCallTimeout)
	}
}

type channelReqMap struct {
	sync.Mutex
	m map[string]*channelReqContexts
}

type RPCClient struct {
	encoder        RPCMessageEncoder
	decoder        RPCMessageDecoder
	channelReqMaps []channelReqMap
}

func (this *RPCClient) addChannelReq(channel RPCChannel, req *reqContext) {
	name := channel.Name()
	m := this.channelReqMaps[util.StringHash(name)%len(this.channelReqMaps)]

	m.Lock()
	defer m.Unlock()
	c, ok := m.m[channel.Name()]
	if !ok {
		c = &channelReqContexts{
			reqs: list.New(),
		}
		m.m[channel.Name()] = c
	}
	c.add(req)
}

func (this *RPCClient) removeChannelReq(req *reqContext) bool {

	name := req.channelName
	m := this.channelReqMaps[util.StringHash(name)%len(this.channelReqMaps)]

	m.Lock()
	defer m.Unlock()

	c, ok := m.m[name]
	if ok {
		ret := c.remove(req)
		if c.reqs.Len() == 0 {
			delete(m.m, name)
		}
		return ret
	} else {
		return false
	}
}

func (this *RPCClient) OnChannelDisconnect(channel RPCChannel) {

	name := channel.Name()
	m := this.channelReqMaps[util.StringHash(name)%len(this.channelReqMaps)]

	m.Lock()
	defer m.Unlock()
	c, ok := m.m[name]
	if ok {
		c.onChannelDisconnect()
	}
}

//收到RPC消息后调用
func (this *RPCClient) OnRPCMessage(message interface{}) {
	if msg, err := this.decoder.Decode(message); nil != err {
		kendynet.GetLogger().Errorf(util.FormatFileLine("RPCClient rpc message decode err:%s\n", err.Error()))
	} else {
		if resp, ok := msg.(*RPCResponse); ok {
			mgr := timerMgrs[msg.GetSeq()%uint64(len(timerMgrs))]
			if ok, ctx := mgr.CancelByIndex(resp.GetSeq()); ok {
				if this.removeChannelReq(ctx.(*reqContext)) {
					ctx.(*reqContext).onResponse(resp.Ret, resp.Err)
				}
			} else if nil == ctx {
				kendynet.GetLogger().Infoln("onResponse with no reqContext", resp.GetSeq())
			}
		} else {
			panic("RPCClient.OnRPCMessage() invaild msg type")
		}
	}
}

//投递，不关心响应和是否失败
func (this *RPCClient) Post(channel RPCChannel, method string, arg interface{}) error {

	req := &RPCRequest{
		Method:   method,
		Seq:      atomic.AddUint64(&sequence, 1),
		Arg:      arg,
		NeedResp: false,
	}

	if request, err := this.encoder.Encode(req); nil != err {
		return fmt.Errorf("encode error:%s\n", err.Error())
	} else {
		if err = channel.SendRequest(request); nil != err {
			return err
		} else {
			return nil
		}
	}
}

func (this *RPCClient) AsynCall(channel RPCChannel, method string, arg interface{}, timeout time.Duration, cb RPCResponseHandler) error {

	if cb == nil {
		panic("cb == nil")
	}

	req := &RPCRequest{
		Method:   method,
		Seq:      atomic.AddUint64(&sequence, 1),
		Arg:      arg,
		NeedResp: true,
	}

	context := &reqContext{
		onResponse:  cb,
		seq:         req.Seq,
		c:           this,
		channelName: channel.Name(),
	}

	if request, err := this.encoder.Encode(req); err != nil {
		return err
	} else {
		mgr := timerMgrs[req.Seq%uint64(len(timerMgrs))]
		this.addChannelReq(channel, context)
		mgr.OnceWithIndex(timeout, context.onTimeout, context, context.seq)
		if err = channel.SendRequest(request); err == nil {
			return nil
		} else {
			this.removeChannelReq(context)
			mgr.CancelByIndex(context.seq)
			return err
		}
	}
}

//同步调用
func (this *RPCClient) Call(channel RPCChannel, method string, arg interface{}, timeout time.Duration) (ret interface{}, err error) {
	respChan := make(chan interface{})
	f := func(ret_ interface{}, err_ error) {
		ret = ret_
		err = err_
		respChan <- nil
	}

	if err = this.AsynCall(channel, method, arg, timeout, f); nil == err {
		_ = <-respChan
	}

	return
}

func NewClient(decoder RPCMessageDecoder, encoder RPCMessageEncoder) *RPCClient {
	if nil == decoder || nil == encoder {
		return nil
	} else {

		client_once.Do(func() {
			timerMgrs = make([]*timer.TimerMgr, 61)
			for i, _ := range timerMgrs {
				timerMgrs[i] = timer.NewTimerMgr(1)
			}
		})

		c := &RPCClient{
			encoder:        encoder,
			decoder:        decoder,
			channelReqMaps: make([]channelReqMap, 127, 127),
		}

		for k, _ := range c.channelReqMaps {
			c.channelReqMaps[k] = channelReqMap{
				m: map[string]*channelReqContexts{},
			}
		}

		return c
	}
}
