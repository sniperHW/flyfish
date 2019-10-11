package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	futil "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/flyfish/util/str"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
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

	this.store.Lock()
	this.store.tmpkvcount--
	this.store.Unlock()

	this.Lock()
	delete(this.tmp, k.uniKey)
	this.Unlock()
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
	this.store.tmpkvcount--
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
	tmpkvcount     int
	kvKickingCount int //当前正在执行kicking的kv数量
	kvNode         *KVNode
	stop           func()
	rn             *raftNode
	lruHead        kv
	lruTail        kv
	storeMgr       *storeMgr
	lruTimer       *timer.Timer
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
	this.Lock()
	defer this.Unlock()
	//未取得租约时不得执行kick
	if this.rn.hasLease() {
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
		this.kvKickingCount++
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
			this.rn.lease.update(this.rn, p.values[0].(int), p.values[1].(uint64))
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
					kv.fields = nil
					Debugln(p.tt, unikey, version, "cache_missing")
				} else {
					kv.setStatus(cache_ok)
					kv.version = version
					fields := p.values[2].([]*proto.Field)
					Debugln(p.tt, unikey, version, "cache_ok", kv.getStatus(), kv.isWriteBack())

					if nil == kv.fields {
						kv.fields = map[string]*proto.Field{}
					}

					for _, v := range fields {
						kv.fields[v.GetName()] = v
					}
				}
				kv.setSnapshoted(true)

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

func (this *kvstore) checkKvCount() bool {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize
	this.Lock()
	defer this.Unlock()

	if this.kvcount+this.tmpkvcount > MaxCachePerGroupSize+MaxCachePerGroupSize/4 {
		return false
	} else {
		return true
	}
}

type kvsnap struct {
	uniKey  string
	fields  map[string]*proto.Field
	version int64
}

func (this *kvsnap) append2Str(s *str.Str) {
	appendProposal2Str(s, proposal_snapshot, this.uniKey, this.version, this.fields)
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

func (this *storeMgr) getkv(table string, key string) (*kv, int32) {

	uniKey := makeUniKey(table, key)

	var k *kv
	var err int32 = errcode.ERR_OK
	var ok bool

	store := this.getStore(uniKey)
	if nil != store {
		slot := store.getSlot(uniKey)
		slot.Lock()

		k, ok = slot.elements[uniKey]
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
					err = errcode.ERR_INVAILD_TABLE
				}
			}
		} else {
			if !store.checkKvCount() {
				//容量限制，不允许再插入新的kv
				err = errcode.ERR_BUSY
			} else {

				meta := this.dbmeta.GetTableMeta(table)
				if meta == nil {
					//err = fmt.Errorf("missing table meta")
					err = errcode.ERR_INVAILD_TABLE
				} else {
					k = newkv(slot, meta, key, uniKey, true)
					slot.tmp[uniKey] = k
					store.Lock()
					store.tmpkvcount++
					store.Unlock()
				}
			}
		}
		slot.Unlock()
	} else {
		fmt.Println("store == nil")
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

func newKVStore(storeMgr *storeMgr, kvNode *KVNode, rn *raftNode, snapshotter *snap.Snapshotter, proposeC *util.BlockQueue, commitC <-chan interface{}, errorC <-chan error, readReqC *util.BlockQueue) *kvstore {

	s := &kvstore{
		proposeC:    proposeC,
		slots:       []*kvSlot{},
		snapshotter: snapshotter,
		readReqC:    readReqC,
		rn:          rn,
		kvNode:      kvNode,
		storeMgr:    storeMgr,
	}

	for i := 0; i < kvSlotSize; i++ {
		s.slots = append(s.slots, &kvSlot{
			elements: map[string]*kv{},
			tmp:      map[string]*kv{},
			store:    s,
		})
	}

	s.lruHead.nnext = &s.lruTail
	s.lruTail.pprev = &s.lruHead

	// replay log into key-value map
	s.readCommits(true, commitC, errorC)
	// read commits from raft into kvStore map until error

	s.lruTimer = timer.Repeat(time.Second, nil, func(t *timer.Timer) {
		s.doLRU()
	})

	go s.readCommits(false, commitC, errorC)
	return s
}

func newStoreMgr(kvnode *KVNode, mutilRaft *mutilRaft, dbmeta *dbmeta.DBMeta, id *int, cluster *string, mask int) *storeMgr {
	mgr := &storeMgr{
		stores: map[int]*kvstore{},
		mask:   mask,
		dbmeta: dbmeta,
	}

	for i := 1; i <= mask; i++ {

		proposeC := util.NewBlockQueue()
		confChangeC := make(chan raftpb.ConfChange)
		readC := util.NewBlockQueue()

		var store *kvstore

		// raft provides a commit stream for the proposals from the http api
		getSnapshot := func() [][]*kvsnap {
			if nil == store {
				return nil
			}
			return store.getSnapshot()
		}

		gotLeaseCb := func() {
			Infoln("got lease")
			//获得租约,强制store对所有kv执行一次sql回写
			for _, v := range store.slots {
				v.Lock()
				for _, vv := range v.elements {
					vv.Lock()
					if !vv.isWriteBack() {
						status := vv.getStatus()
						if status == cache_ok || status == cache_missing {
							vv.setWriteBack(true)
							if status == cache_ok {
								vv.setSqlFlag(sql_insert_update)
							} else if status == cache_missing {
								vv.setSqlFlag(sql_delete)
							}
							Debugln("pushUpdateReq")
							vv.slot.getKvNode().sqlMgr.pushUpdateReq(vv)
						}
					}
					vv.Unlock()
				}
				v.Unlock()
			}
		}

		rn, commitC, errorC, snapshotterReady := newRaftNode(mutilRaft, (*id<<16)+i, strings.Split(*cluster, ","), false, getSnapshot, proposeC, confChangeC, readC, gotLeaseCb)

		store = newKVStore(mgr, kvnode, rn, <-snapshotterReady, proposeC, commitC, errorC, readC)

		store.stop = func() {
			proposeC.Close()
			close(confChangeC)
			readC.Close()
			store.lruTimer.Cancel()
		}

		mgr.addStore(i, store)
	}

	return mgr
}
