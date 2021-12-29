package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flyproto "github.com/sniperHW/flyfish/proto"
	"math"
	"time"
)

type proposalReader struct {
	reader buffer.BufferReader
}

type ppkv struct {
	unikey  string
	version int64
	fields  map[string]*flyproto.Field
}

type pplease struct {
	nodeid  int
	begtime time.Time
}

func appendField(b []byte, field *flyproto.Field) []byte {
	//name len
	b = buffer.AppendUint16(b, uint16(len(field.GetName())))
	//name
	b = buffer.AppendString(b, field.GetName())

	tt := field.GetType()

	b = buffer.AppendByte(b, byte(tt))

	switch tt {
	case flyproto.ValueType_string:
		b = buffer.AppendInt32(b, int32(len(field.GetString())))
		b = buffer.AppendString(b, field.GetString())
	case flyproto.ValueType_float:
		u64 := math.Float64bits(field.GetFloat())
		b = buffer.AppendInt64(b, int64(u64))
	case flyproto.ValueType_int:
		b = buffer.AppendInt64(b, field.GetInt())
	case flyproto.ValueType_blob:
		b = buffer.AppendInt32(b, int32(len(field.GetBlob())))
		b = buffer.AppendBytes(b, field.GetBlob())
	default:
		panic("invaild field value type")
	}
	return b
}

func serilizeSlots(slots *bitmap.Bitmap, b []byte) []byte {
	slotB := slots.ToJson()
	b = buffer.AppendByte(b, byte(proposal_slots))
	b = buffer.AppendInt32(b, int32(len(slotB)))
	return buffer.AppendBytes(b, slotB)
}

func serilizeMeta(meta db.DBMeta, b []byte) []byte {
	metaB, _ := meta.ToJson()
	b = buffer.AppendByte(b, byte(proposal_meta))
	b = buffer.AppendInt32(b, int32(len(metaB)))
	return buffer.AppendBytes(b, metaB)
}

func serilizeLease(b []byte, nodeid int, begtime time.Time) []byte {
	b = buffer.AppendByte(b, byte(proposal_lease))
	b = buffer.AppendInt32(b, int32(nodeid))
	bb, _ := begtime.MarshalBinary()
	b = buffer.AppendInt32(b, int32(len(bb)))
	return buffer.AppendBytes(b, bb)
}

func serilizeKv(b []byte, ptype proposalType, unikey string, version int64, fields map[string]*flyproto.Field) []byte {
	//先写入类型
	b = buffer.AppendByte(b, byte(ptype))
	//len unikey
	b = buffer.AppendUint16(b, uint16(len(unikey)))
	//unikey
	b = buffer.AppendString(b, unikey)

	if ptype != proposal_kick {
		//version
		b = buffer.AppendInt64(b, version)
		//fields
		if nil != fields {
			b = buffer.AppendInt32(b, int32(len(fields)))
			for _, v := range fields {
				b = appendField(b, v)
			}
		} else {
			b = buffer.AppendInt32(b, 0)
		}
	}
	return b
}
