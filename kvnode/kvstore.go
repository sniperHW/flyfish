package kvnode

import (
	"fmt"
	//"encoding/binary"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/proto"
	futil "github.com/sniperHW/flyfish/util"
	//"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	//"go.etcd.io/etcd/raft/raftpb"
	//	"math"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/util/str"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type asynTaskKick struct {
	kv *kv
}

func (this *asynTaskKick) done() {
	this.kv.Lock()
	this.kv.setStatus(cache_remove)
	this.kv.Unlock()
	this.kv.slot.removeKv(this.kv)
}

func (this *asynTaskKick) onError(errno int32) {
	this.kv.Lock()
	this.kv.setKicking(false)
	this.kv.Unlock()
	this.kv.slot.onKickError()
}

func (this *asynTaskKick) append2Str(s *str.Str) {
	appendProposal2Str(s, proposal_kick, this.kv.uniKey)
}

func (this *asynTaskKick) onPorposeTimeout() {

}

var kvSlotSize int = 129

type kvSlot struct {
	sync.Mutex
	tmp      map[string]*kv
	elements map[string]*kv
	store    *kvstore
}

func (this *kvSlot) onKickError() {
	this.store.Lock()
	this.store.kvKickingCount--
	this.store.Unlock()
}

func (this *kvSlot) removeKv(k *kv) {
	this.store.Lock()
	this.store.kvKickingCount--
	this.store.kvcount--
	this.store.removeLRU(k)
	this.store.Unlock()

	this.Lock()
	delete(this.elements, k.uniKey)
	this.Unlock()
}

func (this *kvSlot) removeTmpKv(k *kv) {
	this.Lock()
	defer this.Unlock()
	delete(this.tmp, k.uniKey)
}

func (this *kvSlot) getRaftNode() *raftNode {
	return this.store.rn
}

func (this *kvSlot) getKvNode() *KVNode {
	return this.store.kvNode
}

//发起一致读请求
func (this *kvSlot) issueReadReq(task asynCmdTaskI) {
	this.store.issueReadReq(task)
}

//发起更新请求
func (this *kvSlot) issueUpdate(task asynCmdTaskI) {
	this.store.issueUpdate(task)
}

//请求向所有副本中新增kv
func (this *kvSlot) issueAddkv(task asynCmdTaskI) {
	this.store.issueAddkv(task)
}

func (this *kvSlot) moveTmpkv2OK(kv *kv) {
	this.Lock()
	delete(this.tmp, kv.uniKey)
	this.elements[kv.uniKey] = kv
	this.Unlock()

	this.store.Lock()
	this.store.kvcount++
	this.store.updateLRU(kv)
	this.store.Unlock()
}

// a key-value store backed by raft
type kvstore struct {
	sync.Mutex
	proposeC       *util.BlockQueue
	readReqC       *util.BlockQueue
	snapshotter    *snap.Snapshotter
	slots          []*kvSlot
	kvcount        int //所有slot中len(elements)的总和
	kvKickingCount int //当前正在执行kicking的kv数量
	kvNode         *KVNode
	stop           func()
	rn             *raftNode
	lruHead        kv
	lruTail        kv
	storeMgr       *storeMgr
}

func (this *kvstore) getKvNode() *KVNode {
	return this.kvNode
}

func (this *kvstore) getSlot(uniKey string) *kvSlot {
	return this.slots[futil.StringHash(uniKey)%len(this.slots)]
}

//发起一致读请求
func (this *kvstore) issueReadReq(task asynCmdTaskI) {
	if err := this.readReqC.AddNoWait(task); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}

func (this *kvstore) updateLRU(kv *kv) {

	if kv.nnext != nil || kv.pprev != nil {
		//先移除
		kv.pprev.nnext = kv.nnext
		kv.nnext.pprev = kv.pprev
		kv.nnext = nil
		kv.pprev = nil
	}

	//插入头部
	kv.nnext = this.lruHead.nnext
	kv.nnext.pprev = kv
	kv.pprev = &this.lruHead
	this.lruHead.nnext = kv

}

func (this *kvstore) removeLRU(kv *kv) {
	kv.pprev.nnext = kv.nnext
	kv.nnext.pprev = kv.pprev
	kv.nnext = nil
	kv.pprev = nil
}

func (this *kvstore) doLRU() {
	if this.rn.isLeader() {
		MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize
		if this.lruHead.nnext != &this.lruTail {
			kv := this.lruTail.pprev
			for (this.kvcount - this.kvKickingCount) > MaxCachePerGroupSize {
				if kv == &this.lruHead {
					return
				}
				if !this.tryKick(kv) {
					return
				}
				kv = kv.pprev
			}
		}
	}
}

func (this *kvstore) tryKick(kv *kv) bool {
	kv.Lock()
	if kv.isKicking() {
		kv.Unlock()
		return true
	}

	kickAble := kv.kickable()
	if kickAble {
		kv.setKicking(true)
	}

	kv.Unlock()

	if !kickAble {
		return false
	}

	if err := this.proposeC.AddNoWait(&asynTaskKick{kv: kv}); nil != err {
		this.Lock()
		this.kvKickingCount++
		this.Unlock()
		return true
	} else {
		kv.Lock()
		kv.setKicking(false)
		kv.Unlock()
		return false
	}
}

func (this *kvstore) apply(data []byte) bool {
	this.Lock()
	defer this.Unlock()
	s := str.NewStr(data, len(data))
	offset := 0
	var p *proposal
	for offset < s.Len() {
		p, offset = readProposal(s, offset)
		if nil == p {
			return false
		}

		switch p.tt {
		case proposal_lease:
			this.rn.lease.update(p.values[0].(int))
		case proposal_snapshot, proposal_update, proposal_kick:
			unikey := p.values[0].(string)
			slot := this.slots[futil.StringHash(unikey)%len(this.slots)]
			if p.tt == proposal_kick {
				slot.Lock()
				kv, ok := slot.elements[unikey]
				if !ok {
					slot.Unlock()
					return false
				} else {
					kv.Lock()
					kv.setStatus(cache_remove)
					kv.Unlock()
					this.kvcount--
					this.removeLRU(kv)
					delete(slot.elements, unikey)
					slot.Unlock()
				}
			} else {
				slot.Lock()
				kv, ok := slot.elements[unikey]
				if p.tt == proposal_update && !ok {
					slot.Unlock()
					return false
				}
				version := p.values[1].(int64)

				if !ok {

					tmp := strings.Split(unikey, ":")
					meta := this.storeMgr.dbmeta.GetTableMeta(tmp[0])
					if nil == meta {
						slot.Unlock()
						return false
					}
					kv = newkv(slot, meta, tmp[1], unikey, false)

					this.kvcount++
					slot.elements[unikey] = kv
				}

				kv.Lock()

				if version == 0 {
					kv.setStatus(cache_missing)
				} else {
					kv.setStatus(cache_ok)
					kv.version = version
					fields := p.values[2].([]*proto.Field)
					kv.fields = map[string]*proto.Field{}
					for _, v := range fields {
						kv.fields[v.GetName()] = v
					}
				}
				kv.Unlock()
				slot.Unlock()
				this.updateLRU(kv)
			}
		default:
			return false
		}
	}
	return true
}

func (this *kvstore) readCommits(once bool, commitC <-chan interface{}, errorC <-chan error) {

	for e := range commitC {
		switch e.(type) {
		case *commitedBatchProposal:
			data := e.(*commitedBatchProposal)
			if data == replaySnapshot {
				// done replaying log; new data incoming
				// OR signaled to load snapshot
				snapshot, err := this.snapshotter.Load()
				if err == snap.ErrNoSnapshot {
					return
				}
				if err != nil {
					Fatalln(err)
				}
				Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if !this.apply(snapshot.Data[8:]) {
					Fatalln("recoverFromSnapshot failed")
				}
			} else if data == replayOK {
				if once {
					Infoln("apply ok,keycount", this.kvcount)
					return
				} else {
					continue
				}
			} else {
				data.apply(this)
			}
		case *readBatchSt:
			e.(*readBatchSt).reply()
		}
	}

	if err, ok := <-errorC; ok {
		Fatalln(err)
	}
}

type kvsnap struct {
	uniKey  string
	fields  map[string]*proto.Field
	version int64
}

func (this *kvsnap) append2Str(s *str.Str) {

}

func (this *kvstore) getSnapshot() [][]*kvsnap {

	beg := time.Now()

	ret := make([][]*kvsnap, 0, len(this.slots))

	ch := make(chan []*kvsnap)

	for i := 0; i < len(this.slots); i++ {
		go func(i int) {
			slot := this.slots[i]
			slot.Lock()
			kvsnaps := make([]*kvsnap, 0, len(slot.elements))
			for _, v := range slot.elements {
				v.Lock()
				status := v.getStatus()
				if status == cache_ok || status == cache_missing {
					snap := &kvsnap{
						uniKey:  v.uniKey,
						version: v.version,
					}

					if v.fields != nil {
						snap.fields = map[string]*proto.Field{}
						for kk, vv := range v.fields {
							snap.fields[kk] = vv
						}
					}
					kvsnaps = append(kvsnaps, snap)
				}
				v.Unlock()
			}
			slot.Unlock()
			ch <- kvsnaps
		}(i)
	}

	for i := 0; i < len(this.slots); i++ {
		v := <-ch
		ret = append(ret, v)
	}

	Infoln("clone time", time.Now().Sub(beg))

	return ret

}

type storeMgr struct {
	sync.RWMutex
	stores map[int]*kvstore
	mask   int
	dbmeta *dbmeta.DBMeta
}

func (this *storeMgr) getkv(table string, key string) (*kv, error) {

	uniKey := makeUniKey(table, key)

	var k *kv = nil
	var err error

	store := this.getStore(uniKey)
	if nil != store {
		slot := store.getSlot(uniKey)
		slot.Lock()

		k, ok := slot.elements[uniKey]
		if !ok {
			k, ok = slot.tmp[uniKey]
		}

		if ok {
			if !this.dbmeta.CheckMetaVersion(k.meta.Version()) {
				newMeta := this.dbmeta.GetTableMeta(table)
				if newMeta != nil {
					atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&k.meta)), unsafe.Pointer(newMeta))
				} else {
					//log error
					err = fmt.Errorf("missing table meta")
				}
			}
		} else {

			meta := this.dbmeta.GetTableMeta(table)
			if meta == nil {
				err = fmt.Errorf("missing table meta")
			} else {
				k = newkv(slot, meta, key, uniKey, true)
				slot.tmp[uniKey] = k
			}
		}

		slot.Unlock()
	}

	return k, err
}

func (this *storeMgr) getStore(uniKey string) *kvstore {
	this.RLock()
	defer this.RUnlock()
	index := (futil.StringHash(uniKey) % this.mask) + 1
	return this.stores[index]
}

func (this *storeMgr) addStore(index int, store *kvstore) bool {
	if 0 == index || nil == store {
		Fatalln("0 == index || nil == store")
	}
	this.Lock()
	defer this.Unlock()
	_, ok := this.stores[index]
	if ok {
		return false
	}
	this.stores[index] = store
	return true
}

func (this *storeMgr) stop() {
	this.RLock()
	defer this.RUnlock()
	for _, v := range this.stores {
		v.stop()
	}
}

func newStoreMgr(mutilRaft *mutilRaft, dbmeta *dbmeta.DBMeta) (*storeMgr, error) {
	return nil, nil
}
