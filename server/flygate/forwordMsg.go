package flygate

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	"sync"
	"sync/atomic"
	"time"
)

type cacheI interface {
	removeMsg(e *forwordMsg)
}

type cache struct {
	l *list.List
}

func (c *cache) removeMsg(m *forwordMsg) {
	if nil != m.listElement {
		c.l.Remove(m.listElement)
	}
	m.listElement = nil
	m.clearCache()
}

func (c *cache) addMsg(m *forwordMsg) int {
	m.listElement = c.l.PushBack(m)
	m.setCache(c)
	return c.l.Len()
}

func (c *cache) lenMsg() int {
	return c.l.Len()
}

type emtpyCache struct {
}

func (ec emtpyCache) removeMsg(m *forwordMsg) {

}

type deadlineTimer struct {
	sync.Mutex
	timer *time.Timer
}

func (dt *deadlineTimer) set(timer *time.Timer) {
	dt.Lock()
	if nil != dt.timer {
		dt.timer.Stop()
	}
	dt.timer = timer
	dt.Unlock()
}

func (dt *deadlineTimer) stop() {
	dt.Lock()
	if nil != dt.timer {
		dt.timer.Stop()
	}
	dt.timer = nil
	dt.Unlock()
}

type forwordMsg struct {
	leaderVersion   int64
	dict            *map[int64]*forwordMsg
	listElement     *list.Element
	cache           atomic.Value
	seqno           int64
	slot            int
	oriSeqno        int64 //来自客户端的seqno
	cmd             uint16
	bytes           []byte
	deadline        time.Time
	deadlineTimer   deadlineTimer
	replyer         *replyer
	replied         int32
	totalPendingReq *int64
	store           uint64 //high32:setid,low32:storeid
}

//atomic.Value无法存实现接口的不同类型，只用将接口类型放在cachePtr中，把cachePtr存进atomic.Value
type cachePtr struct {
	c cacheI
}

func (r *forwordMsg) clearCache() {
	r.setCache(emtpyCache{})
}

func (r *forwordMsg) setCache(c cacheI) {
	r.cache.Store(cachePtr{c: c})
}

func (r *forwordMsg) cacheRemove() {
	if v := r.cache.Load(); nil != v {
		v.(cachePtr).c.removeMsg(r)
	}
}

func (r *forwordMsg) reply(b []byte) {
	if atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		r.deadlineTimer.stop()
		r.cacheRemove()
		r.replyer.reply(b)
	}
}

func (r *forwordMsg) replyErr(err errcode.Error) {
	if atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		r.deadlineTimer.stop()
		r.cacheRemove()
		r.replyer.replyErr(r.oriSeqno, r.cmd, err)
	}
}

func (r *forwordMsg) dropReply() {
	if atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		r.deadlineTimer.stop()
		r.cacheRemove()
		r.replyer.dropReply()
	}
}
