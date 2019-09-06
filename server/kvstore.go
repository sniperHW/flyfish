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

package raft

import (
	"encoding/binary"
	//"fmt"
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

var kvSlotSize int = 19

type kvSlot struct {
	kv  map[string]*cacheKey
	mtx sync.Mutex
}

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- *batchBinlog // channel for proposing updates
	snapshotter *snap.Snapshotter
	slots       []kvSlot //map[string]*cacheKey
	mtx         sync.Mutex
	lruHead     cacheKey
	lruTail     cacheKey
	ctxs        *ctxArray
	nextFlush   time.Time
	binlogStr   *str
	batchCount  int32 //待序列化到文件的binlog数量
	keySize     int
}

var caches *kvstore

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- *batchBinlog, commitC <-chan *commitedBatchBinlog, errorC <-chan error) *kvstore {

	config := conf.GetConfig()

	s := &kvstore{
		proposeC:    proposeC,
		slots:       []kvSlot{}, //map[string]*cacheKey{},
		snapshotter: snapshotter,
		nextFlush:   time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval)),
	}

	for i := 0; i < kvSlotSize; i++ {
		s.slots = append(s.slots, kvSlot{
			kv: map[string]*cacheKey{},
		})
	}

	s.lruHead.nnext = &s.lruTail
	s.lruTail.pprev = &s.lruHead

	// replay log into key-value map
	s.readCommits(true, commitC, errorC)
	// read commits from raft into kvStore map until error

	timer.Repeat(time.Millisecond*time.Duration(config.FlushInterval), nil, func(t *timer.Timer) {
		if isStop() {
			t.Cancel()
		} else {
			s.mtx.Lock()
			s.tryCommitBatch()
			s.mtx.Unlock()
		}
	})

	timer.Repeat(time.Second, nil, func(t *timer.Timer) {
		if isStop() {
			t.Cancel()
		} else {
			s.mtx.Lock()
			s.kickCacheKey()
			s.mtx.Unlock()
		}
	})

	go s.readCommits(false, commitC, errorC)
	return s
}

//func (s *kvstore) getKV(unikey string) *cacheKey {
//	slot := s.kv[StringHash(unikey%len(s.kv)]
//}

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
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for s.keySize > MaxCachePerGroupSize && s.lruHead.nnext != &s.lruTail {
		c := s.lruTail.pprev
		if !s.kick(c) {
			return
		}
	}
}

func (s *kvstore) kick(ckey *cacheKey) bool {
	kickAble := false
	ckey.mtx.Lock()
	kickAble = ckey.kickAbleNoLock()
	if kickAble {
		ckey.lockCmdQueue()
	}
	ckey.mtx.Unlock()
	if !kickAble {
		return false
	}

	if nil == s.ctxs {
		s.ctxs = ctxArrayGet()
	}

	ctx := &cmdContext{
		command: &command{
			cmdType: cmdKick,
			uniKey:  ckey.uniKey,
			ckey:    ckey,
		},
	}

	s.ctxs.append(ctx)

	s.appendBinLog(binlog_kick, ckey.uniKey, nil, 0)

	s.tryCommitBatch()

	return true
}

func (s *kvstore) Propose(propose *batchBinlog) {
	//fmt.Println("-----------------------Propose--------------------")
	s.proposeC <- propose
}

func (s *kvstore) readCommits(once bool, commitC <-chan *commitedBatchBinlog, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
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
			if !s.recoverFromSnapshot(snapshot.Data[8:]) {
				log.Panic("recoverFromSnapshot failed")
			}
			if once {
				return
			} else {
				continue
			}
		}

		if !data.localPropose {
			s.applyLeaderPropose(data.data)
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
						ckey.mtx.Unlock()
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
			}
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

	ret := make([][]kvsnap, len(s.slots))

	ch := make(chan []kvsnap)

	for i := 0; i < len(s.slots); i++ {
		go func(i int) {
			slot := s.slots[i]
			slot.mtx.Lock()
			kvsnaps := make([]kvsnap, len(slot.kv))
			for _, v := range slot.kv {
				v.mtx.Lock()
				if v.status == cache_ok || v.status == cache_missing {
					v.snapshoted = true

					if v.uniKey == "" {
						panic("uniKey is nil")
					}

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

	/*	s.mtx.Lock()

		kvsnaps := make([]kvsnap, len(s.kv))

		beg := time.Now()

		for _, v := range s.kv {
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

		Infoln("clone time", time.Now().Sub(beg))

		s.mtx.Unlock()

		return kvsnaps
	*/

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

func (s *kvstore) applyLeaderPropose(data []byte) bool {
	return s.recoverFromSnapshot(data)
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	offset := 0
	recordCount := 0
	n := len(snapshot)

	for offset < n {

		newOffset, tt, unikey, version, values := readBinLog(snapshot, offset)

		//Infoln(newOffset, tt, unikey, version, values)

		offset = newOffset

		slot := s.slots[StringHash(unikey)%len(s.slots)]

		slot.mtx.Lock()

		ckey, _ := slot.kv[unikey]
		recordCount++

		if tt == binlog_snapshot {
			if nil == ckey {
				tmp := strings.Split(unikey, ":")
				ckey = newCacheKey(s, &slot, tmp[0], strings.Join(tmp[1:], ""), unikey)
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
			}
		} else if tt == binlog_kick {
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

func initKVStore(id *int, cluster *string) {

	proposeC := make(chan *batchBinlog)
	confChangeC := make(chan raftpb.ConfChange)

	// raft provides a commit stream for the proposals from the http api
	getSnapshot := func() [][]kvsnap {
		if nil == caches {
			return nil
		}
		return caches.getSnapshot()
	}

	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), false, getSnapshot, proposeC, confChangeC)

	caches = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

}
