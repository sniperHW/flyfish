// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/timer"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

var cmdProcessor cmdProcessorI

type cmdProcessorI interface {
	processCmd(*cacheKey, bool)
}

type ctxArray struct {
	count int
	ctxs  []*cmdContext
}

func (this *ctxArray) append(ctx *cmdContext) {
	//FlushCount可能因为重载配置文件变大，导致原有空间不足
	if this.count >= len(this.ctxs) {
		ctxs := make([]*cmdContext, conf.GetConfig().FlushCount)
		copy(ctxs, this.ctxs)
		this.ctxs = ctxs
	}
	this.ctxs[this.count] = ctx
	this.count++
}

func (this *ctxArray) full() bool {
	return this.count == cap(this.ctxs)
}

var ctxArrayPool = sync.Pool{
	New: func() interface{} {
		return &ctxArray{
			ctxs:  make([]*cmdContext, conf.GetConfig().FlushCount),
			count: 0,
		}
	},
}

func ctxArrayGet() *ctxArray {
	return ctxArrayPool.Get().(*ctxArray)
}

func ctxArrayPut(w *ctxArray) {
	w.count = 0
	ctxArrayPool.Put(w)
}

var kvSlotSize int = 129

type kvSlot struct {
	kv  map[string]*cacheKey
	mtx sync.Mutex
}

type proposeBatch struct {
	ctxs       *ctxArray
	nextFlush  time.Time
	binlogStr  *str
	batchCount int32 //待序列化到文件的binlog数量
	timer      *timer.Timer
}

type readBatch struct {
	ctxs       *ctxArray
	nextFlush  time.Time
	batchCount int32 //待序列化到文件的binlog数量
	timer      *timer.Timer
}

type readBatchSt struct {
	readIndex int64 //for linearizableRead
	ctxs      *ctxArray
	deadline  time.Time
}

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- *batchBinlog // channel for proposing updates
	readReqC    chan<- *readBatchSt
	snapshotter *snap.Snapshotter
	slots       []*kvSlot //map[string]*cacheKey
	mtx         sync.Mutex
	lruHead     cacheKey
	lruTail     cacheKey

	keySize      int
	kickingCount int
	stop         func()

	lruTimer *timer.Timer
	rn       *raftNode

	proposeBatch proposeBatch
	readBatch    readBatch
}

type storeGroup struct {
	sync.RWMutex
	stores map[int]*kvstore
	mod    int
}

func (this *storeGroup) getStore(uniKey string) *kvstore {
	this.RLock()
	defer this.RUnlock()
	index := (StringHash(uniKey) % this.mod) + 1
	return this.stores[index]
}

func (this *storeGroup) addStore(index int, store *kvstore) bool {
	if 0 == index || nil == store {
		panic("0 == index || nil == store")
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

func (this *storeGroup) stop() {
	this.RLock()
	defer this.RUnlock()
	for _, v := range this.stores {
		v.stop()
	}
}

func (this *storeGroup) tryProposeBatch() {
	this.RLock()
	defer this.RUnlock()
	for _, v := range this.stores {
		v.tryProposeBatch()
	}
}

func newKVStore(rn *raftNode, snapshotter *snap.Snapshotter, proposeC chan<- *batchBinlog, commitC <-chan interface{}, errorC <-chan error, readReqC chan<- *readBatchSt) *kvstore {

	config := conf.GetConfig()

	s := &kvstore{
		proposeC:    proposeC,
		slots:       []*kvSlot{},
		snapshotter: snapshotter,
		proposeBatch: proposeBatch{
			nextFlush: time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval)),
		},
		readBatch: readBatch{
			nextFlush: time.Now().Add(time.Millisecond * time.Duration(config.ReadFlushInterval)),
		},
		readReqC: readReqC,
		rn:       rn,
	}

	for i := 0; i < kvSlotSize; i++ {
		s.slots = append(s.slots, &kvSlot{
			kv: map[string]*cacheKey{},
		})
	}

	s.lruHead.nnext = &s.lruTail
	s.lruTail.pprev = &s.lruHead

	// replay log into key-value map
	s.readCommits(true, commitC, errorC)
	// read commits from raft into kvStore map until error

	s.proposeBatch.timer = timer.Repeat(time.Millisecond*time.Duration(config.FlushInterval), nil, func(t *timer.Timer) {
		s.mtx.Lock()
		s.tryProposeBatch()
		s.mtx.Unlock()
	})

	s.readBatch.timer = timer.Repeat(time.Millisecond*time.Duration(config.ReadFlushInterval), nil, func(t *timer.Timer) {
		s.mtx.Lock()
		s.tryReadBatch()
		s.mtx.Unlock()
	})

	s.lruTimer = timer.Repeat(time.Second, nil, func(t *timer.Timer) {
		s.mtx.Lock()
		s.kickCacheKey()
		s.mtx.Unlock()
	})

	go s.readCommits(false, commitC, errorC)
	return s
}

func (s *kvstore) updateLRU(ckey *cacheKey) {

	if ckey.nnext != nil || ckey.pprev != nil {
		//先移除
		ckey.pprev.nnext = ckey.nnext
		ckey.nnext.pprev = ckey.pprev
		ckey.nnext = nil
		ckey.pprev = nil
	}

	//插入头部
	ckey.nnext = s.lruHead.nnext
	ckey.nnext.pprev = ckey
	ckey.pprev = &s.lruHead
	s.lruHead.nnext = ckey

}

func (s *kvstore) removeLRU(ckey *cacheKey) {
	ckey.pprev.nnext = ckey.nnext
	ckey.nnext.pprev = ckey.pprev
	ckey.nnext = nil
	ckey.pprev = nil
}

func (s *kvstore) kickCacheKey() {
	if s.rn.isLeader() {
		MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

		if s.lruHead.nnext != &s.lruTail {
			c := s.lruTail.pprev
			for (s.keySize - s.kickingCount) > MaxCachePerGroupSize {
				if c == &s.lruHead {
					return
				}
				if !s.kick(c) {
					return
				}
				c = c.pprev
			}
		}
	}
}

func (s *kvstore) kick(ckey *cacheKey) bool {
	kickAble := false
	ckey.mtx.Lock()
	if ckey.kicking {
		ckey.mtx.Unlock()
		return true
	}

	kickAble = ckey.kickAbleNoLock()
	if kickAble {
		ckey.kicking = true
		ckey.lockCmdQueue()
	}
	ckey.mtx.Unlock()

	if !kickAble {
		return false
	}

	if nil == s.proposeBatch.ctxs {
		s.proposeBatch.ctxs = ctxArrayGet()
	}

	ctx := &cmdContext{
		command: &command{
			cmdType: cmdKick,
			uniKey:  ckey.uniKey,
			ckey:    ckey,
		},
	}

	s.kickingCount++

	s.proposeBatch.ctxs.append(ctx)

	s.appendBinLog(binlog_kick, ckey.uniKey, nil, 0)

	s.tryProposeBatch()

	return true
}

func (s *kvstore) Propose(propose *batchBinlog) {
	s.proposeC <- propose
}

func (s *kvstore) tryReadBatch() {
	if s.readBatch.batchCount > 0 {

		config := conf.GetConfig()

		if s.readBatch.batchCount >= int32(config.ReadFlushCount) || time.Now().After(s.readBatch.nextFlush) {

			s.readBatch.batchCount = 0

			ctxs := s.readBatch.ctxs

			s.readBatch.ctxs = nil

			s.readReqC <- &readBatchSt{
				ctxs: ctxs,
			}
		}
	}
}

func (s *kvstore) issueReadReq(c *cmdContext) {
	s.mtx.Lock()

	if nil == s.readBatch.ctxs {
		s.readBatch.ctxs = ctxArrayGet()
	}

	s.readBatch.batchCount++

	if s.readBatch.batchCount == 1 {
		s.readBatch.nextFlush = time.Now().Add(time.Millisecond * time.Duration(conf.GetConfig().ReadFlushInterval))
	}

	s.readBatch.ctxs.append(c)

	s.tryReadBatch()

	s.mtx.Unlock()

}

func (s *kvstore) readCommits(once bool, commitC <-chan interface{}, errorC <-chan error) {
	for d := range commitC {
		switch d.(type) {
		case *commitedBatchBinlog:
			data := d.(*commitedBatchBinlog)
			if data == replaySnapshot {
				// done replaying log; new data incoming
				// OR signaled to load snapshot
				snapshot, err := s.snapshotter.Load()
				if err == snap.ErrNoSnapshot {
					return
				}
				if err != nil {
					log.Panic(err)
				}
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if !s.replay(snapshot.Data[8:]) {
					log.Panic("recoverFromSnapshot failed")
				}
			} else if data == replayOK {
				if once {
					Infoln("replay ok,keycount", s.keySize)
					return
				} else {
					continue
				}
			} else {

				//本地提交的propose直接使用ctxs中内容apply
				if nil != data.ctxs {
					for i := 0; i < data.ctxs.count; i++ {
						v := data.ctxs.ctxs[i]
						if v.getCmdType() != cmdKick {
							v.reply(errcode.ERR_OK, v.fields, v.version)
							ckey := v.getCacheKey()
							ckey.mtx.Lock()
							ckey.snapshoted = true

							if v.writeBackFlag == write_back_insert || v.writeBackFlag == write_back_update || v.writeBackFlag == write_back_insert_update {
								ckey.setValueNoLock(v)
								ckey.setOKNoLock(v.version)
							} else {
								ckey.setMissingNoLock()
							}

							ckey.sqlFlag = v.writeBackFlag

							if !ckey.writeBackLocked {
								ckey.writeBackLocked = true
								pushSqlWriteReq(ckey)
							}
							ckey.mtx.Unlock()
						}
					}

					for i := 0; i < data.ctxs.count; i++ {
						v := data.ctxs.ctxs[i]
						if v.getCmdType() == cmdKick {
							queueCmdSize := 0
							ckey := v.getCacheKey()
							ckey.mtx.Lock()
							queueCmdSize = ckey.cmdQueue.Len()
							if queueCmdSize > 0 {
								/*
								 *   kick执行完之后，对这个key又有新的访问请求
								 *   此时必须把snapshoted设置为true,这样后面的变更请求才能以snapshot记录到日志中
								 *   否则，重放日志时因为kick先执行，变更重放将因为找不到key出错
								 */
								ckey.snapshoted = false
								ckey.kicking = false
							}
							ckey.mtx.Unlock()
							s.mtx.Lock()
							s.kickingCount--
							s.mtx.Unlock()
							if queueCmdSize > 0 {
								ckey.processQueueCmd()
							} else {
								s.mtx.Lock()
								s.removeLRU(ckey)
								s.keySize--
								s.mtx.Unlock()
								ckey.slot.mtx.Lock()
								delete(ckey.slot.kv, ckey.uniKey)
								ckey.slot.mtx.Unlock()
							}
						} else {
							v.getCacheKey().processQueueCmd()
						}
					}

					ctxArrayPut(data.ctxs)
				} else {
					s.replay(data.data)
				}
			}
		case *readBatchSt:
			c := d.(*readBatchSt)
			for i := 0; i < c.ctxs.count; i++ {
				v := c.ctxs.ctxs[i]
				ckey := v.getCacheKey()
				if !s.rn.isLeader() {
					v.reply(errcode.ERR_NOT_LEADER, nil, -1)
				} else {
					ckey.mtx.Lock()
					if ckey.status == cache_missing {
						v.reply(errcode.ERR_NOTFOUND, nil, -1)
					} else {
						v.reply(errcode.ERR_OK, ckey.values, ckey.version)
					}
					ckey.mtx.Unlock()
				}
				ckey.processQueueCmd()
			}
			ctxArrayPut(c.ctxs)
		}

	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

type kvsnap struct {
	uniKey  string
	values  map[string]*proto.Field
	version int64
}

func (s *kvstore) getSnapshot() [][]kvsnap {

	beg := time.Now()

	ret := make([][]kvsnap, 0, len(s.slots))

	ch := make(chan []kvsnap)

	for i := 0; i < len(s.slots); i++ {
		go func(i int) {
			slot := s.slots[i]
			slot.mtx.Lock()
			kvsnaps := make([]kvsnap, 0, len(slot.kv))
			for _, v := range slot.kv {
				v.mtx.Lock()
				if v.status == cache_ok || v.status == cache_missing {
					v.snapshoted = true

					s := kvsnap{
						uniKey:  v.uniKey,
						version: v.version,
					}

					if v.values != nil {
						s.values = map[string]*proto.Field{}
						for kk, vv := range v.values {
							s.values[kk] = vv
						}
					}
					kvsnaps = append(kvsnaps, s)
				}
				v.mtx.Unlock()
			}
			slot.mtx.Unlock()
			ch <- kvsnaps
		}(i)
	}

	for i := 0; i < len(s.slots); i++ {
		v := <-ch
		ret = append(ret, v)
	}

	Infoln("clone time", time.Now().Sub(beg))

	return ret

}

func readBinLog(buffer []byte, offset int) (int, int, string, int64, map[string]*proto.Field) {
	tt := int(buffer[offset])
	offset += 1
	l := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
	offset += 4
	uniKey := string(buffer[offset : offset+l])
	offset += l
	version := int64(binary.BigEndian.Uint64(buffer[offset : offset+8]))
	offset += 8

	var values map[string]*proto.Field

	valueSize := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
	offset += 4

	//Infoln(unikey, version, valueSize)

	if valueSize > 0 {
		values = map[string]*proto.Field{}
		for i := 0; i < valueSize; i++ {
			l := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
			offset += 4
			name := string(buffer[offset : offset+l])
			offset += l

			vType := proto.ValueType(int(buffer[offset]))
			offset += 1

			switch vType {
			case proto.ValueType_string:
				l = int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
				offset += 4
				values[name] = proto.PackField(name, string(buffer[offset:offset+l]))
				offset += l
			case proto.ValueType_float:
				u64 := binary.BigEndian.Uint64(buffer[offset : offset+8])
				values[name] = proto.PackField(name, math.Float64frombits(u64))
				offset += 8
			case proto.ValueType_int:
				values[name] = proto.PackField(name, int64(binary.BigEndian.Uint64(buffer[offset:offset+8])))
				offset += 8
			case proto.ValueType_uint:
				values[name] = proto.PackField(name, uint64(binary.BigEndian.Uint64(buffer[offset:offset+8])))
				offset += 8
			case proto.ValueType_blob:
				l = int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
				offset += 4
				v := make([]byte, l)
				copy(v, buffer[offset:offset+l])
				values[name] = proto.PackField(name, v)
				offset += l
			default:
				panic("invaild value type")
			}
		}
	}

	return offset, tt, uniKey, version, values
}

func (s *kvstore) replay(data []byte) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	offset := 0
	recordCount := 0
	n := len(data)

	for offset < n {

		newOffset, tt, unikey, version, values := readBinLog(data, offset)

		offset = newOffset

		slot := s.slots[StringHash(unikey)%len(s.slots)]

		slot.mtx.Lock()

		ckey, _ := slot.kv[unikey]
		recordCount++

		if tt == binlog_snapshot {
			if nil == ckey {
				tmp := strings.Split(unikey, ":")
				ckey = newCacheKey(s, slot, tmp[0], strings.Join(tmp[1:], ""), unikey)
				s.keySize++
				slot.kv[unikey] = ckey
				s.updateLRU(ckey)
			}
			ckey.mtx.Lock()
			ckey.values = values
			ckey.version = version
			ckey.snapshoted = true
			if ckey.version == 0 {
				ckey.sqlFlag = write_back_delete
				ckey.status = cache_missing
			} else {
				ckey.sqlFlag = write_back_insert_update
				ckey.status = cache_ok
			}
			ckey.mtx.Unlock()
		} else if tt == binlog_update {
			if ckey != nil {
				if ckey.status != cache_ok || ckey.values == nil {
					Fatalln("invaild tt", unikey, tt, recordCount, offset)
					slot.mtx.Unlock()
					return false
				}
				ckey.mtx.Lock()
				for k, v := range values {
					ckey.values[k] = v
				}
				ckey.version = version
				ckey.sqlFlag = write_back_insert_update
				ckey.mtx.Unlock()
				s.updateLRU(ckey)
			} else {
				panic("binlog_update key == nil")
			}
		} else if tt == binlog_delete {
			if ckey != nil {
				if ckey.status != cache_ok {
					slot.mtx.Unlock()
					Fatalln("invaild tt", unikey, tt, recordCount, offset)
					return false
				}
				ckey.mtx.Lock()
				ckey.values = nil
				ckey.version = version
				ckey.status = cache_missing
				ckey.sqlFlag = write_back_delete
				ckey.mtx.Unlock()
				s.updateLRU(ckey)
			} else {
				panic("binlog_delete key == nil")
			}
		} else if tt == binlog_kick {
			/*
			 *   ckey == nil 是合法的
			 *   例如,leader接到一个get请求，在本地内存插入一对kv,但因为没有对数据做变更，所以不会生成一个proposal,也就不会将
			 *   这个事件同步到副本。
			 *   过了一段时间,leader要kick这个kv,kick操作将同步到所有副本，此时副本内存中并没有这个kv
			 */
			if ckey != nil {
				s.keySize--
				s.removeLRU(ckey)
				delete(slot.kv, unikey)
			}
		} else {
			slot.mtx.Unlock()
			Fatalln("invaild tt", unikey, tt, recordCount, offset)
			return false
		}
		slot.mtx.Unlock()
	}
	return true
}

func initKvGroup(mutilRaft *mutilRaft, id *int, cluster *string, mod int) *storeGroup {

	storeGroup := &storeGroup{
		stores: map[int]*kvstore{},
		mod:    mod,
	}

	for i := 1; i <= mod; i++ {

		proposeC := make(chan *batchBinlog)
		confChangeC := make(chan raftpb.ConfChange)

		var store *kvstore

		// raft provides a commit stream for the proposals from the http api
		getSnapshot := func() [][]kvsnap {
			if nil == store {
				return nil
			}
			return store.getSnapshot()
		}

		rn, commitC, errorC, snapshotterReady, readC := newRaftNode(mutilRaft, (*id<<16)+i, strings.Split(*cluster, ","), false, getSnapshot, proposeC, confChangeC)

		store = newKVStore(rn, <-snapshotterReady, proposeC, commitC, errorC, readC)

		store.stop = func() {
			close(proposeC)
			close(confChangeC)
			store.proposeBatch.timer.Cancel()
			store.readBatch.timer.Cancel()
			store.lruTimer.Cancel()
		}

		storeGroup.addStore(i, store)
	}

	return storeGroup

}
