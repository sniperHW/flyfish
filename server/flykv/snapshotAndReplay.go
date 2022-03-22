package flykv

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/bloomfilter"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/pkg/raft"
	flyproto "github.com/sniperHW/flyfish/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"runtime"
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

func (s *kvstore) replayFromBytes(b []byte) error {

	var err error

	bb := b[len(b)-1]
	b = b[:len(b)-1]

	var dc compress.DecompressorI

	defer func() {
		if nil != dc {
			releaseDecompressor(dc)
		}
	}()

	if bb == byte(1) {
		dc = getDecompressor()
		b, err = dc.Decompress(b)
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

		switch ptype {
		case proposal_slot_transfer:
			p := data.(*SlotTransferProposal)
			if p.transferType == slotTransferIn {
				filter := &bloomfilter.Filter{}
				if err := filter.UnmarshalBinaryZip(p.filter); nil != err {
					return err
				}

				s.slots[p.slot] = &slot{
					kvMap:  map[string]*kv{},
					filter: filter,
				}
			} else if p.transferType == slotTransferOut {
				s.slots[p.slot] = nil
			}
		case proposal_suspend:
			s.halt = true
		case proposal_resume:
			s.halt = false
		case proposal_filters:
			for k, v := range data.([]*[]byte) {
				if nil != v {
					slot := &slot{
						kvMap:  map[string]*kv{},
						filter: &bloomfilter.Filter{},
					}
					if err := slot.filter.UnmarshalBinaryZip(*v); nil != err {
						return err
					}
					s.slots[k] = slot
				}
			}
		case proposal_meta:
			data.(db.DBMeta).MoveTo(s.meta)
		case proposal_nop:
		case proposal_last_writeback_version:
			p := data.(*LastWriteBackVersionProposal)
			if kv := s.getkv(sslot.Unikey2Slot(p.kv.uniKey), p.kv.uniKey); nil != kv {
				kv.lastWriteBackVersion = p.version
			}
		case proposal_snapshot, proposal_kick, proposal_update:
			p := data.(*kv)

			slot := sslot.Unikey2Slot(p.uniKey)
			kv := s.getkv(slot, p.uniKey)

			if nil == kv {
				if ptype != proposal_snapshot {
					return fmt.Errorf("bad data,%s with a bad proposal_type:%v", p.uniKey, ptype)
				} else {
					var err errcode.Error
					table, key := splitUniKey(p.uniKey)
					if kv, err = s.newkv(slot, p.uniKey, key, table); nil != err {
						return fmt.Errorf("bad data,%s is no table define", p.uniKey)
					}
				}
			}

			switch ptype {
			case proposal_kick:
				s.deleteKv(kv)
			case proposal_update:
				kv.version = p.version
				for k, v := range p.fields {
					kv.fields[k] = v
				}
			case proposal_snapshot:
				kv.version = p.version
				kv.lastWriteBackVersion = p.lastWriteBackVersion
				kv.fields = p.fields
				if kv.version > 0 {
					kv.state = kv_ok
				} else {
					kv.state = kv_no_record
				}

				if kv.lastWriteBackVersion == 0 && !s.slots[kv.slot].filter.ContainsWithHashs(kv.hash) {
					s.slots[kv.slot].filter.AddWithHashs(kv.hash)
				}

			}
			GetSugar().Debugf("%s ok", p.uniKey)
		}
	}
}

func (s *kvstore) makeSnapshot(notifyer *raft.SnapshotNotify) {
	var groupSize int = s.kvnode.config.SnapshotCurrentCount
	if 0 == groupSize {
		groupSize = runtime.NumCPU()
	}

	beg := time.Now()

	snaps := make([][]*kv, groupSize)

	filters := make([]*bloomfilter.Filter, len(s.slots))

	{
		var waitGroup sync.WaitGroup
		waitGroup.Add(groupSize)
		//多线程序列化和压缩
		for i := 0; i < groupSize; i++ {
			go func(i int) {
				for j := i; j < len(s.slots); j += groupSize {
					if nil != s.slots[j] {
						filters[j] = s.slots[j].filter.Clone()
					}
				}
				waitGroup.Done()
			}(i)
		}
		waitGroup.Wait()
	}

	{
		var waitGroup sync.WaitGroup
		waitGroup.Add(groupSize)
		//多线程序列化和压缩
		for i := 0; i < groupSize; i++ {
			go func(i int) {
				var snapkv []*kv
				for j := i; j < len(s.slots); j += groupSize {
					if nil != s.slots[j] {
						for _, v := range s.slots[j].kvMap {
							kv := &kv{
								uniKey:               v.uniKey,
								version:              v.version,
								fields:               map[string]*flyproto.Field{},
								lastWriteBackVersion: v.lastWriteBackVersion,
							}

							for kk, vv := range v.fields {
								kv.fields[kk] = vv
							}

							snapkv = append(snapkv, kv)
						}
					}
				}
				snaps[i] = snapkv
				waitGroup.Done()
			}(i)
		}

		waitGroup.Wait()
	}

	GetSugar().Infof("traval all kv pairs and filters take: %v", time.Now().Sub(beg))

	go func() {

		beg = time.Now()

		buff := make([]byte, 0, buffsize)
		ll := len(buff)
		buff = buffer.AppendInt32(buff, 0) //占位符
		buff = serilizeHalt(s.halt, buff)
		buff = serilizeMeta(s.meta, buff)
		buff = buffer.AppendByte(buff, byte(proposal_filters))
		c := 0
		for i := 0; i < len(s.slots); i++ {
			if nil != s.slots {
				c++
			}
		}

		buff = buffer.AppendInt32(buff, int32(c))
		var mtx sync.Mutex

		{
			var waitGroup sync.WaitGroup
			waitGroup.Add(groupSize)
			//多线程序列化和压缩
			for i := 0; i < groupSize; i++ {
				go func(i int) {
					for j := i; j < len(filters); j += groupSize {
						if nil != filters[j] {
							filter, _ := filters[j].MarshalBinaryZip()
							mtx.Lock()
							buff = buffer.AppendInt32(buff, int32(j))
							buff = buffer.AppendInt32(buff, int32(len(filter)))
							buff = buffer.AppendBytes(buff, filter)
							mtx.Unlock()
						}
					}
					waitGroup.Done()
				}(i)
			}
			waitGroup.Wait()
			buff = append(buff, byte(0)) //写入无压缩标记
			binary.BigEndian.PutUint32(buff[ll:ll+4], uint32(len(buff)-ll-4))
		}

		{
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
						b = serilizeKv(b, proposal_snapshot, vv.uniKey, vv.version, vv.lastWriteBackVersion, vv.fields)
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
		}

		GetSugar().Infof("Snapshot len:%d serilize use:%v", len(buff), time.Now().Sub(beg))

		notifyer.Notify(buff)
	}()
}
