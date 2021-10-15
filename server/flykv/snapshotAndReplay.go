package flykv

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"sync"
	"time"
)

type snapshotReader struct {
	reader buffer.BufferReader
}

func newSnapshotReader(b []byte) *snapshotReader {
	return &snapshotReader{
		reader: buffer.NewReader(b),
	}
}

func (this *snapshotReader) read() (isOver bool, data []byte, err error) {
	if this.reader.IsOver() {
		isOver = true
		return
	} else {
		var l int32
		l, err = this.reader.CheckGetInt32()
		if nil != err {
			return
		}
		data, err = this.reader.CheckGetBytes(int(l))
		if nil != err {
			return
		}
		return
	}
}

const buffsize = 1024 * 16 * 1024

const maxBlockSize = 1024 * 1024 * 1

func compressSnap(b []byte) []byte {
	c := getCompressor()
	cb, err := c.Compress(b[4:])
	if nil != err {
		GetSugar().Errorf("snapshot compress error:%v", err)
		b = append(b, byte(0))
		binary.BigEndian.PutUint32(b[:4], uint32(len(b)-4))
	} else {
		b = b[:4]
		b = append(b, cb...)
		b = append(b, byte(1))
		binary.BigEndian.PutUint32(b[:4], uint32(len(cb)+1))
	}
	releaseCompressor(c)
	return b
}

func (s *kvstore) snapMerge(snaps ...[]byte) ([]byte, error) {

	store := make([]map[string]*kv, len(s.keyvals))

	for i := 0; i < len(store); i++ {
		store[i] = map[string]*kv{}
	}

	var lease pplease

	var slots *bitmap.Bitmap

	snapBytes := [][]byte{}

	for _, b := range snaps {

		r := newSnapshotReader(b)
		var data []byte
		var isOver bool
		var err error
		for {
			isOver, data, err = r.read()
			if isOver {
				break
			} else if nil != err {
				return nil, err
			} else {
				snapBytes = append(snapBytes, data)
			}
		}
	}

	u := getUnCompressor()

	defer releaseUnCompressor(u)

	for _, b := range snapBytes {
		var err error

		bb := b[len(b)-1]
		b = b[:len(b)-1]
		if bb == byte(1) {
			b, err = u.UnCompress(b)
			if nil != err {
				GetSugar().Errorf("UnCompress error %v", err)
				return nil, err
			}
		}

		r := newProposalReader(b)

		var ptype proposalType
		var data interface{}
		var isOver bool
		for {
			isOver, ptype, data, err = r.read()
			if nil != err {
				return nil, err
			} else if isOver {
				break
			}

			if ptype == proposal_lease {
				lease = data.(pplease)
			} else if ptype == proposal_slots {
				slots = data.(*bitmap.Bitmap)
			} else {
				p := data.(ppkv)
				groupID := sslot.StringHash(p.unikey) % len(store)
				keyvalue, ok := store[groupID][p.unikey]

				if !ok {
					if ptype == proposal_kick {
						continue
					} else if ptype == proposal_update {
						return nil, fmt.Errorf("bad data,%s with a bad proposal_type:%d", p.unikey, ptype)
					} else {
						keyvalue = &kv{
							uniKey:  p.unikey,
							groupID: groupID,
						}
						store[groupID][p.unikey] = keyvalue
					}
				}

				switch ptype {
				case proposal_kick:
					delete(store[keyvalue.groupID], p.unikey)
				case proposal_update:
					keyvalue.version = p.version
					for k, v := range p.fields {
						keyvalue.fields[k] = v
					}

				case proposal_snapshot:
					keyvalue.version = p.version
					keyvalue.fields = p.fields
				}
			}
		}
	}

	beg := time.Now()

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(store))

	buff := make([]byte, 0, buffsize)

	//先写入slot
	buff = serilizeSlots(slots, buff)

	var mtx sync.Mutex

	kvcount := 0

	for _, v := range store {
		kvcount += len(v)
	}

	//多线程序列化和压缩
	for _, v := range store {
		go func(m map[string]*kv) {
			b := make([]byte, 0, maxBlockSize*2)
			b = buffer.AppendInt32(b, 0) //占位符
			for _, vv := range m {
				b = serilizeKv(b, proposal_snapshot, vv.uniKey, vv.version, vv.fields)
				if len(b) >= maxBlockSize {
					b = compressSnap(b)
					mtx.Lock()
					buff = append(buff, b...)
					mtx.Unlock()
					b = b[0:0]
					b = buffer.AppendInt32(b, 0) //占位符
				}
			}

			if len(b) > 4 {
				b = compressSnap(b)
				mtx.Lock()
				buff = append(buff, b...)
				mtx.Unlock()
			}

			waitGroup.Done()

		}(v)
	}

	waitGroup.Wait()

	ll := len(buff)
	buff = buffer.AppendInt32(buff, 0) //占位符
	buff = serilizeLease(buff, lease.nodeid, lease.begtime)
	buff = append(buff, byte(0)) //写入无压缩标记
	binary.BigEndian.PutUint32(buff[ll:ll+4], uint32(len(buff)-ll-4))

	GetSugar().Infof("snapMerge: %v,snapshot len:%d", time.Now().Sub(beg), len(buff))

	return buff, nil
}

func (s *kvstore) replayFromBytes(callByReplaySnapshot bool, b []byte) error {

	var err error

	bb := b[len(b)-1]
	b = b[:len(b)-1]

	var u compress.UnCompressorI

	defer func() {
		if nil != u {
			releaseUnCompressor(u)
		}
	}()

	if bb == byte(1) {
		u = getUnCompressor()
		b, err = u.UnCompress(b)
		if nil != err {
			GetSugar().Errorf("UnCompress error %v", err)
			return err
		}
	}

	r := newProposalReader(b)

	var ptype proposalType
	var data interface{}
	var isOver bool
	for {
		isOver, ptype, data, err = r.read()
		if nil != err {
			return err
		} else if isOver {
			return nil
		}

		if ptype == proposal_lease {
			p := data.(pplease)
			s.lease.update(p.nodeid, p.begtime)
		} else if ptype == proposal_slot_transfer {
			p := data.(*SlotTransferProposal)
			p.store = s
			p.apply()
		} else if ptype == proposal_slots {
			s.slots = data.(*bitmap.Bitmap)
		} else {
			p := data.(ppkv)
			groupID := sslot.StringHash(p.unikey) % len(s.keyvals)
			keyvalue, ok := s.keyvals[groupID].kv[p.unikey]
			GetSugar().Debugf("%s %d %d", p.unikey, ptype, p.version)
			if !ok {
				if ptype == proposal_kick {
					if callByReplaySnapshot {
						continue
					} else {
						/*
						 *  在回放log entry的情况下是否会出违反约束的情况?
						 *
						 *  在没有bug的情况log entry回放分两种情况：
						 *
						 *  1不存在snapshot：如果kv不存在kick不会执行，因此不会被写入到log entry。
						 *
						 *  2存在snapshot:合并的snapshot表示store的一个内存状态。后面跟随log entry。
						 * 	添加一个在合并的snapshot中kv不存在，那么此时kv在内存中必然不存在。那么在后续的log entry就不可能存在
						 *  一个kick,因为kick不会被执行。除非在kick之前的log entry中存在一个将kv添加到store的entry。
						 *
						 *  log entry回放必定在合并snapshot回放之后执行，因此，如果程序没有bug,在执行log entry回放的时候
						 *  一定不会违反这个约束。
						 */
						return fmt.Errorf("bad data,%s with a bad proposal_type:%d", p.unikey, ptype)
					}
				} else if ptype == proposal_update {
					return fmt.Errorf("bad data,%s with a bad proposal_type:%d", p.unikey, ptype)
				} else {
					var e errcode.Error
					table, key := splitUniKey(p.unikey)
					slot := sslot.Unikey2Slot(p.unikey)
					keyvalue, e = s.newkv(slot, groupID, p.unikey, key, table)
					if nil != e {
						return fmt.Errorf("bad data,%s is no table define", p.unikey)
					}
				}
			}

			switch ptype {
			case proposal_kick:
				s.deleteKv(keyvalue, !callByReplaySnapshot)
			case proposal_update:
				keyvalue.version = p.version
				for k, v := range p.fields {
					keyvalue.fields[k] = v
				}
				if !callByReplaySnapshot {
					keyvalue.snapshot = true
				}

				s.lru.updateLRU(&keyvalue.lru)

			case proposal_snapshot:
				keyvalue.version = p.version
				keyvalue.fields = p.fields
				if keyvalue.version != 0 {
					keyvalue.state = kv_ok
				} else {
					keyvalue.state = kv_no_record
				}

				if !callByReplaySnapshot {
					delete(s.keyvals[groupID].kicks, p.unikey)
					keyvalue.snapshot = true
				}

				s.lru.updateLRU(&keyvalue.lru)

			}
			GetSugar().Debugf("%s ok", p.unikey)
		}
	}
}

func (s *kvstore) makeSnapshot(notifyer *raft.SnapshotNotify) {
	beg := time.Now()

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(s.keyvals))

	snaps := make([][]*kv, len(s.keyvals))

	buff := make([]byte, 0, buffsize)

	//写入slots
	buff = serilizeSlots(s.slots, buff)

	buff = s.lease.snapshot(buff)

	//多线程序列化和压缩
	for i, _ := range s.keyvals {
		go func(i int, m *kvmgr) {
			/*
			 *  考虑在执行snapshot之前先变更某个kv,之后将其kick
			 *  在执行snapshot的时候kv在m.kv中已经被删除，但存在于m.kicks中
			 *
			 *  在这种情况下是否可以忽略对kv的proposal_kick写入snapshot?
			 *
			 *  答案是不行，因为可能前一个snapshot中存在这个kv,如果忽略proposal_kick
			 *  那么在后面的snapshot merge中就kv就无法被剔除。
			 *
			 *  但是，如果不忽略proposal_kick,在snapshot重放的过程中（replayFromBytes以及snapMerge）就可能
			 *  出现kick kv但kv实际不存在的情况()。
			 *
			 *  为了处理这种情况执行snapMerge以及当replayFromBytes传入callByReplaySnapshot=true时，将直接忽略这种情况。
			 *
			 */

			var snapkvs []*kv

			for _, v := range m.kv {
				if v.snapshot {
					v.snapshot = false
					skv := &kv{
						uniKey:  v.uniKey,
						version: v.version,
						fields:  map[string]*flyproto.Field{},
					}

					for kk, vv := range v.fields {
						skv.fields[kk] = vv
					}

					snapkvs = append(snapkvs, skv)
				}
			}

			for k, _ := range m.kicks {
				snapkvs = append(snapkvs, &kv{
					uniKey: k,
					state:  kv_invaild,
				})
			}

			m.kicks = map[string]bool{}

			snaps[i] = snapkvs

			waitGroup.Done()

		}(i, &s.keyvals[i])
	}

	waitGroup.Wait()

	GetSugar().Infof("traval all kv pairs and serilize take: %v", time.Now().Sub(beg))

	go func() {
		var mtx sync.Mutex
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(snaps))
		for _, snapkvs := range snaps {
			go func(snapkvs []*kv) {
				b := make([]byte, 0, maxBlockSize*2)
				b = buffer.AppendInt32(b, 0) //占位符

				/*
				 *  每当块大小超过maxBlockSize就执行一次压缩
				 *  避免内存扩张过大
				 */

				for _, vv := range snapkvs {
					if vv.state == kv_invaild {
						b = serilizeKv(b, proposal_kick, vv.uniKey, 0, nil)
					} else {
						b = serilizeKv(b, proposal_snapshot, vv.uniKey, vv.version, vv.fields)
					}

					if len(b) >= maxBlockSize {
						b = compressSnap(b)
						mtx.Lock()
						buff = append(buff, b...)
						mtx.Unlock()
						b = b[0:0]
						b = buffer.AppendInt32(b, 0) //占位符
					}
				}

				if len(b) > 4 {
					b = compressSnap(b)
					mtx.Lock()
					buff = append(buff, b...)
					mtx.Unlock()
				}

				waitGroup.Done()

			}(snapkvs)
		}
		waitGroup.Wait()

		GetSugar().Infof("Snapshot len:%s", len(buff))

		notifyer.Notify(buff)
	}()
}

// //func (s *kvstore) getSnapshot() ([]byte, error) {

// //	beg := time.Now()

// //	var waitGroup sync.WaitGroup
// //	waitGroup.Add(len(s.keyvals))

// //	buff := make([]byte, 0, buffsize)

// 	//写入slots
// //	buff = serilizeSlots(s.slots, buff)

// //	var mtx sync.Mutex

// 	//多线程序列化和压缩
// //	for i, _ := range s.keyvals {
// //		go func(m *kvmgr) {
// //			b := make([]byte, 0, buffsize)
// //			b = buffer.AppendInt32(b, 0) //占位符

// 			/*
// 			 *  考虑在执行snapshot之前先变更某个kv,之后将其kick
// 			 *  在执行snapshot的时候kv在m.kv中已经被删除，但存在于m.kicks中
// 			 *
// 			 *  在这种情况下是否可以忽略对kv的proposal_kick写入snapshot?
// 			 *
// 			 *  答案是不行，因为可能前一个snapshot中存在这个kv,如果忽略proposal_kick
// 			 *  那么在后面的snapshot merge中就kv就无法被剔除。
// 			 *
// 			 *  但是，如果不忽略proposal_kick,在snapshot重放的过程中（replayFromBytes以及snapMerge）就可能
// 			 *  出现kick kv但kv实际不存在的情况()。
// 			 *
// 			 *  为了处理这种情况执行snapMerge以及当replayFromBytes传入callByReplaySnapshot=true时，将直接忽略这种情况。
// 			 *
// 			 */

// //			for _, v := range m.kv {
// //				if v.snapshot {
// //					v.snapshot = false
// //					b = serilizeKv(b, proposal_snapshot, v.uniKey, v.version, v.fields)
// //				}
// //			}

// //			for k, _ := range m.kicks {
// //				b = serilizeKv(b, proposal_kick, k, 0, nil)
// //			}

// 			m.kicks = map[string]bool{}

// 			c := getCompressor()

// 			cb, err := c.Compress(b[4:])
// 			if nil != err {
// 				GetSugar().Errorf("snapshot compress error:%v", err)
// 				b = append(b, byte(0))
// 				binary.BigEndian.PutUint32(b[:4], uint32(len(b)-4))
// 			} else {
// 				b = b[:4]
// 				b = append(b, cb...)
// 				b = append(b, byte(1))
// 				binary.BigEndian.PutUint32(b[:4], uint32(len(cb)+1))
// 			}
// 			releaseCompressor(c)

// 			mtx.Lock()
// 			buff = append(buff, b...)
// 			mtx.Unlock()

// 			waitGroup.Done()

// 		}(&s.keyvals[i])
// 	}

// 	waitGroup.Wait()

// 	buff = s.lease.snapshot(buff)

// 	GetSugar().Infof("getSnapshot: %v,snapshot len:%d", time.Now().Sub(beg), len(buff))

// 	return buff, nil
// }
