package kvnode

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	sslot "github.com/sniperHW/flyfish/server/slot"
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

const buffsize = 1024 * 64 * 1024

var snapshotBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, buffsize)
	},
}

func getSnapshotBuffer() []byte {
	return snapshotBufferPool.Get().([]byte)
}

func releaseSnapshotBuffer(b []byte) {
	snapshotBufferPool.Put(b[:0])
}

func (s *kvstore) snapMerge(snaps ...[]byte) ([]byte, error) {

	store := make([]map[string]*kv, len(s.keyvals))

	for i := 0; i < len(store); i++ {
		store[i] = map[string]*kv{}
	}

	var lease pplease
	//var tbmeta []byte

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

	for _, b := range snapBytes {
		var err error

		compress := b[len(b)-1]
		b = b[:len(b)-1]
		if compress == byte(1) {
			b, err = s.unCompressor.Clone().UnCompress(b)
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

			if ptype == proposal_tbmeta {
				//tbmeta = data.([]byte)
			} else if ptype == proposal_lease {
				lease = data.(pplease)
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
					if keyvalue.version != 0 {
						keyvalue.state = kv_ok
					} else {
						keyvalue.state = kv_no_record
					}
				}
			}
		}
	}

	beg := time.Now()

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(store))

	buff := make([]byte, 0, buffsize)
	var mtx sync.Mutex

	kvcount := 0

	for _, v := range store {
		kvcount += len(v)
	}

	//多线程序列化和压缩
	for i, v := range store {
		go func(id int, m map[string]*kv) {
			b := getSnapshotBuffer()
			b = buffer.AppendInt32(b, 0) //占位符

			for _, vv := range m {
				b = serilizeKv(b, proposal_snapshot, vv.uniKey, vv.version, vv.fields)
			}

			compressor := s.snapCompressor.Clone()

			cb, err := compressor.Compress(b[4:])
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

			mtx.Lock()
			buff = append(buff, b...)
			mtx.Unlock()

			releaseSnapshotBuffer(b)

			waitGroup.Done()

		}(i, v)
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

	compress := b[len(b)-1]
	b = b[:len(b)-1]
	if compress == byte(1) {
		b, err = s.unCompressor.Clone().UnCompress(b)
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

		if ptype == proposal_tbmeta {
			def, err := db.CreateDbDefFromJsonString(data.([]byte))
			if nil != err {
				return err
			}

			meta, err := s.kvnode.metaCreator(def)

			if nil != err {
				return err
			}

			if meta.GetVersion() > s.meta.GetVersion() {
				s.meta = meta
			}

		} else if ptype == proposal_lease {
			p := data.(pplease)
			s.lease.update(p.nodeid, p.begtime)
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
						 * 	加入在合并的snapshot中kv不存在，那么此时kv在内存中必然不存在。那么在后续的log entry就不可能存在
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
					keyvalue, e = s.newkv(groupID, p.unikey, key, table)
					if nil != e {
						return fmt.Errorf("bad data,%s is no table define", p.unikey)
					}
					s.keyvals[groupID].kv[p.unikey] = keyvalue
				}
			}

			switch ptype {
			case proposal_kick:
				delete(s.keyvals[groupID].kv, p.unikey)
				if !callByReplaySnapshot {
					s.keyvals[groupID].kicks[p.unikey] = true
				}
			case proposal_update:
				keyvalue.version = p.version
				for k, v := range p.fields {
					keyvalue.fields[k] = v
				}
				if !callByReplaySnapshot {
					keyvalue.snapshot = true
				}
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
			}
			GetSugar().Debugf("%s ok", p.unikey)
		}
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {

	beg := time.Now()

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(s.keyvals))

	buff := make([]byte, 0, buffsize)
	var mtx sync.Mutex

	//多线程序列化和压缩
	for i, _ := range s.keyvals {
		go func(m *kvmgr) {
			b := getSnapshotBuffer()
			b = buffer.AppendInt32(b, 0) //占位符

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
			 *  出现kick kv但kv实际不存在的情况。
			 *
			 *  为了处理这种情况执行snapMerge以及当replayFromBytes传入callByReplaySnapshot=true时，将直接忽略这种情况。
			 *
			 */

			for _, v := range m.kv {
				if v.snapshot {
					v.snapshot = false
					b = serilizeKv(b, proposal_snapshot, v.uniKey, v.version, v.fields)
				}
			}

			for k, _ := range m.kicks {
				b = serilizeKv(b, proposal_kick, k, 0, nil)
			}

			m.kicks = map[string]bool{}

			compressor := s.snapCompressor.Clone()

			cb, err := compressor.Compress(b[4:])
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

			mtx.Lock()
			buff = append(buff, b...)
			mtx.Unlock()

			releaseSnapshotBuffer(b)

			waitGroup.Done()

		}(&s.keyvals[i])
	}

	waitGroup.Wait()

	buff = s.lease.snapshot(buff)

	GetSugar().Infof("getSnapshot: %v,snapshot len:%d", time.Now().Sub(beg), len(buff))

	return buff, nil
}
