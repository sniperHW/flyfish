package bitfield

import (
	"fmt"
)

var InvaildField = fmt.Errorf("InvaildField")

type Field32 struct {
	mask   uint32
	offset uint32
}

func MakeFiled32(mask uint32) Field32 {

	if mask == 0 {
		panic("invaild mask")
	}

	offset := uint32(0)
	for i := uint32(0); i < uint32(31); i++ {
		if mask&(uint32(1)<<i) != 0 {
			offset = i
			break
		}
	}

	gotZero := false

	//mask必须是连续的1,中间不能有0间隔
	for i := offset + 1; i < uint32(31); i++ {
		if mask&(uint32(1)<<i) == 0 {
			gotZero = true
		} else if gotZero {
			panic("invaild mask")
		}
	}

	return Field32{
		mask:   mask,
		offset: offset,
	}

}

type BitField32 struct {
	data   uint32
	fields []*Field32
}

func NewBitField32(fields ...Field32) *BitField32 {

	b32 := &BitField32{
		fields: make([]*Field32, 32),
	}

	tmp := uint32(0)

	for _, v := range fields {
		if tmp&v.mask != 0 {
			return nil
		}
		tmp |= v.mask
		b32.fields[v.offset] = &Field32{
			mask:   v.mask,
			offset: v.offset,
		}
	}

	return b32
}

func (this *BitField32) Set(field Field32, v uint32) error {
	f := this.fields[field.offset]
	if f == nil || f.offset != field.offset || f.mask != field.mask {
		return InvaildField
	} else {
		tmp := (uint32(0xffffffff) ^ field.mask) & this.data
		this.data = tmp | (field.mask & (v << field.offset))
		return nil
	}
}

func (this *BitField32) Get(field Field32) (uint32, error) {
	f := this.fields[field.offset]
	if f == nil || f.offset != field.offset || f.mask != field.mask {
		return 0, InvaildField
	} else {
		return (this.data & field.mask) >> field.offset, nil
	}
}
