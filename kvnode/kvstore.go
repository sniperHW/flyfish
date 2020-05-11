package kvnode

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	futil "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/flyfish/util/str"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type asynTaskKick struct {
	kv *kv
}

func (this *asynTaskKick) done() {
	logger.Debugln("kick done set cache_remove", this.kv.uniKey)
	this.kv.store.removeKv(this.kv)
}

func (this *asynTaskKick) onError(errno int32) {
	this.kv.Lock()
	this.kv.setKicking(false)
	this.kv.Unlock()
}

func (this *asynTaskKick) append2Str(s *str.Str) {
	appendProposal2Str(s, proposal_kick, this.kv.uniKey)
}

func (this *asynTaskKick) onPorposeTimeout() {
	this.onError(errcode.ERR_TIMEOUT)
}

type leaseNotify int

var snapGroupSize int = 129

// a key-value store backed by raft
type kvstore struct {
	sync.Mutex
	proposeC     *util.BlockQueue
	readReqC     *util.BlockQueue
	elements     map[string]*kv
	kvNode       *KVNode
	stop         func()
	rn           *raftNode
	lruHead      kv
	lruTail      kv
	storeMgr     *storeMgr
	lruTimer     *timer.Timer
	unCompressor net.UnCompressorI
}

func (this *kvstore) getKvNode() *KVNode {
	return this.kvNode
}

func (this *kvstore) getRaftNode() *raftNode {
	return this.rn
}

func (this *kvstore) removeKv(k *kv) {

	processAgain := false

	this.Lock()
	k.Lock()

	if !k.cmdQueue.empty() {
		k.resetStatus()
		processAgain = true
	} else {
		k.setStatus(cache_remove)
		this.removeLRU(k)
		delete(this.elements, k.uniKey)
	}

	k.Unlock()
	this.Unlock()

	if processAgain {
		k.processCmd(nil)
	}
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
			count := 0
			for len(this.elements)-count > MaxCachePerGroupSize {
				if kv == &this.lruHead {
					return
				}

				ok, removeDirect := this.tryKick(kv)
				if !ok {
					return
				}

				prev := kv.pprev
				if removeDirect {
					this.removeLRU(kv)
					kv.setStatus(cache_remove)
					delete(this.elements, kv.uniKey)
				} else {
					count++
				}

				kv = prev
			}
		}
	}
}

func (this *kvstore) kick(taskKick *asynCmdTaskKick) bool {
	kv := taskKick.getKV()
	kv.setKicking(true)
	if err := this.proposeC.AddNoWait(taskKick); nil == err {
		return true
	} else {
		return false
	}
}

func (this *kvstore) tryKick(kv *kv) (bool, bool) {

	removeDirect := false

	kv.Lock()
	defer kv.Unlock()
	if kv.isKicking() {
		return true, removeDirect
	}

	if kv.kickable() {
		kv.setKicking(true)
	} else {
		return false, removeDirect
	}

	if kv.getStatus() == cache_new {
		removeDirect = true
		return true, removeDirect
	}

	if err := this.proposeC.AddNoWait(&asynTaskKick{kv: kv}); nil == err {
		return true, removeDirect
	} else {
		kv.setKicking(false)
		return false, removeDirect
	}
}

func splitUniKey(s string) (table string, key string) {
	i := -1
	for k, v := range s {
		if v == 58 {
			i = k
			break
		}
	}

	if i >= 0 {
		table = s[:i]
		key = s[i+1:]
	}

	return
}

func (this *kvstore) apply(data []byte, snapshot bool) bool {

	compressFlag := binary.BigEndian.Uint16(data[:2])

	if compressFlag == compressMagic {
		var err error
		data, err = this.unCompressor.UnCompress(data[2:])
		if nil != err {
			logger.Errorln("uncompress error")
			return false
		}
	} else {
		data = data[2:]
	}

	s := str.NewStr(data, len(data))

	offset := 0

	this.Lock()
	defer this.Unlock()

	if snapshot {
		this.elements = map[string]*kv{}
		this.lruHead.nnext = &this.lruTail
		this.lruTail.pprev = &this.lruHead
	}

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

			if p.tt == proposal_kick {
				logger.Debugln(unikey, "cache_kick")
				kv, ok := this.elements[unikey]
				if !ok {
					return false
				} else {
					this.removeLRU(kv)
					delete(this.elements, unikey)
				}
			} else {
				kv, ok := this.elements[unikey]
				if p.tt == proposal_update && !ok {
					return false
				}

				version := p.values[1].(int64)

				if !ok {

					table, key := splitUniKey(unikey)
					meta := this.storeMgr.dbmeta.GetTableMeta(table)
					if nil == meta {
						return false
					}
					kv = newkv(this, meta, key, unikey, false)
					this.elements[unikey] = kv
				}

				if version == 0 {
					kv.setStatus(cache_missing)
					kv.fields = nil
					logger.Debugln(p.tt, unikey, version, "cache_missing", kv.fields)
				} else {
					kv.setStatus(cache_ok)
					kv.version = version
					fields := p.values[2].([]*proto.Field)

					logger.Debugln(p.tt, unikey, version, "cache_ok", kv.getStatus(), kv.isWriteBack(), fields)

					if nil == kv.fields {
						kv.fields = map[string]*proto.Field{}
					}

					for _, v := range fields {
						//不一致表示数据库字段类型发生变更，老数据直接丢弃
						if !kv.meta.CheckFieldMeta(v) {
							logger.Debugln("drop field", v.GetName())
						} else {
							kv.fields[v.GetName()] = v
						}
					}
				}
				kv.setSnapshoted(true)

				this.updateLRU(kv)
			}
		default:
			return false
		}
	}
	return true
}

func (this *kvstore) readCommits(snapshotter *snap.Snapshotter, commitC <-chan interface{}, errorC <-chan error) {

	for e := range commitC {
		switch e.(type) {
		case *commitedBatchProposal:
			data := e.(*commitedBatchProposal)
			if data == replaySnapshot {
				// done replaying log; new data incoming
				// OR signaled to load snapshot
				snapshot, err := snapshotter.Load()
				if err != nil {
					logger.Fatalln(err)
				} else {
					logger.Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
					if !this.apply(snapshot.Data[8:], true) {
						logger.Fatalln("recoverFromSnapshot failed")
					}
				}
			} else if data == replayOK {
				logger.Infoln("reply ok,keycount", len(this.elements))
				return
			} else {
				data.apply(this)
			}
		case *readBatchSt:
			e.(*readBatchSt).reply()
		case leaseNotify:
			this.gotLease()
		}

	}

	if err, ok := <-errorC; ok {
		logger.Fatalln(err)
	}
}

func (this *kvstore) checkKvCount() bool {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize
	if len(this.elements) > MaxCachePerGroupSize {
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

	ret := make([][]*kvsnap, 0, snapGroupSize)

	snapGroup := make([][]*kv, snapGroupSize, snapGroupSize)

	ch := make(chan []*kvsnap, snapGroupSize)

	this.Lock()
	defer this.Unlock()

	//根据key对kv分组
	for k, v := range this.elements {
		i := futil.StringHash(k) % snapGroupSize
		snapGroup[i] = append(snapGroup[i], v)
	}

	//并行序列化每组中的kv
	for i := 0; i < snapGroupSize; i++ {
		go func(i int) {
			kvsnaps := make([]*kvsnap, 0, len(this.elements))
			for _, v := range snapGroup[i] {
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
			ch <- kvsnaps
		}(i)
	}

	for i := 0; i < snapGroupSize; i++ {
		v := <-ch
		ret = append(ret, v)
	}

	logger.Infoln("clone time", time.Now().Sub(beg))

	return ret

}

func (this *kvstore) gotLease() {
	this.Lock()
	defer this.Unlock()
	//获得租约,强制store对所有kv执行一次sql回写
	for _, vv := range this.elements {
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
				logger.Debugln("pushUpdateReq", vv.uniKey, status, vv.fields)
				this.kvNode.sqlMgr.pushUpdateReq(vv)
			}
		}
		vv.Unlock()
	}
}

type storeMgr struct {
	sync.RWMutex
	stores map[int]*kvstore
	mask   int
	dbmeta *dbmeta.DBMeta
}

func (this *storeMgr) getkvOnly(table string, key string, uniKey string) *kv {
	store := this.getStore(uniKey)
	if store != nil {
		store.Lock()
		defer store.Unlock()
		return store.elements[uniKey]
	} else {
		return nil
	}
}

func (this *storeMgr) getkv(table string, key string, uniKey string) (*kv, int32) {
	var k *kv
	var ok bool
	var err int32 = errcode.ERR_OK
	store := this.getStore(uniKey)
	if nil != store {
		store.Lock()
		defer store.Unlock()
		k, ok = store.elements[uniKey]
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
					err = errcode.ERR_INVAILD_TABLE
				} else {
					k = newkv(store, meta, key, uniKey, true)
					store.elements[uniKey] = k
					store.updateLRU(k)
				}
			}
		}
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
		logger.Fatalln("0 == index || nil == store")
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

func newKVStore(storeMgr *storeMgr, kvNode *KVNode, proposeC *util.BlockQueue, readReqC *util.BlockQueue) *kvstore {

	s := &kvstore{
		proposeC:     proposeC,
		elements:     map[string]*kv{},
		readReqC:     readReqC,
		kvNode:       kvNode,
		storeMgr:     storeMgr,
		unCompressor: &net.ZipUnCompressor{},
	}

	s.lruHead.nnext = &s.lruTail
	s.lruTail.pprev = &s.lruHead

	return s
}

func newStoreMgr(kvnode *KVNode, mutilRaft *mutilRaft, dbmeta *dbmeta.DBMeta, id *int, peers map[int]string, mask int) *storeMgr {
	mgr := &storeMgr{
		stores: map[int]*kvstore{},
		mask:   mask,
		dbmeta: dbmeta,
	}

	for i := 1; i <= mask; i++ {

		proposeC := util.NewBlockQueue()
		confChangeC := make(chan raftpb.ConfChange)
		readC := util.NewBlockQueue()

		store := newKVStore(mgr, kvnode, proposeC, readC)

		rn, commitC, errorC, snapshotterReady := newRaftNode(mutilRaft, (*id<<16)+i, peers, false, proposeC, confChangeC, readC, store.getSnapshot)

		store.rn = rn

		store.lruTimer = timer.Repeat(time.Second, nil, func(t *timer.Timer, _ interface{}) {
			store.doLRU()
		}, nil)

		store.stop = func() {
			proposeC.Close()
			close(confChangeC)
			readC.Close()
			store.lruTimer.Cancel()
		}

		snapshotter := <-snapshotterReady

		store.readCommits(snapshotter, commitC, errorC)

		go store.readCommits(snapshotter, commitC, errorC)

		mgr.addStore(i, store)
	}

	return mgr
}
