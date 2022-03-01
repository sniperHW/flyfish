package flykv

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/db"
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

		if ptype == proposal_slot_transfer {
			p := data.(*SlotTransferProposal)
			if p.transferType == slotTransferIn {
				s.slots.Set(p.slot)
			} else if p.transferType == slotTransferOut {
				s.slots.Clear(p.slot)
			}
		} else if ptype == proposal_slots {
			s.slots = data.(*bitmap.Bitmap)
		} else if ptype == proposal_meta {
			data.(db.DBMeta).MoveTo(s.meta)
		} else if ptype == proposal_nop {
			s.lastLeader = data.(uint64)
		} else if ptype == proposal_last_writeback_version {
			p := data.(*kv)
			groupID := sslot.StringHash(p.uniKey) % len(s.kv)
			kv, ok := s.kv[groupID][p.uniKey]
			if ok {
				kv.lastWriteBackVersion = p.lastWriteBackVersion
			}
		} else {
			p := data.(*kv)
			groupID := sslot.StringHash(p.uniKey) % len(s.kv)
			kv, ok := s.kv[groupID][p.uniKey]
			if !ok {
				if ptype == proposal_kick {
					return fmt.Errorf("bad data,%s with a bad proposal_type:kick", p.uniKey)
				} else if ptype == proposal_update {
					return fmt.Errorf("bad data,%s with a bad proposal_type:update", p.uniKey)
				} else {
					var e errcode.Error
					slot := sslot.Unikey2Slot(p.uniKey)
					table, key := splitUniKey(p.uniKey)
					kv, e = s.newAppliedKv(slot, groupID, p.uniKey, key, table)
					if nil != e {
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
			}
			GetSugar().Debugf("%s ok", p.uniKey)
		}
	}
}

func (s *kvstore) makeSnapshot(notifyer *raft.SnapshotNotify) {
	beg := time.Now()

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(s.kv))

	snaps := make([][]*kv, len(s.kv))

	buff := make([]byte, 0, buffsize)

	{
		ll := len(buff)
		buff = buffer.AppendInt32(buff, 0) //占位符
		buff = serilizeSlots(s.slots, buff)
		buff = serilizeMeta(s.meta, buff)
		buff = append(buff, byte(0)) //写入无压缩标记
		binary.BigEndian.PutUint32(buff[ll:ll+4], uint32(len(buff)-ll-4))
	}

	//多线程序列化和压缩
	for i, v := range s.kv {
		go func(i int, m map[string]*kv) {
			var snapkv []*kv
			for _, v := range m {
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
			snaps[i] = snapkv

			waitGroup.Done()

		}(i, v)
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
					//GetSugar().Infof("snapshot.serilizeKv %s %d %d", vv.uniKey, vv.version, vv.lastWriteBackVersion)
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

		GetSugar().Infof("Snapshot len:%d", len(buff))

		notifyer.Notify(buff)
	}()
}
