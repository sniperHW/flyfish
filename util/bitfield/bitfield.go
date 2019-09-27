package bitfield

type BitField32 struct {
	data uint32
}

func (this *BitField32) Set(mask uint32, offset uint32, v uint32) {
	this.data |= (mask & (v << offset))
}

func (this *BitField32) Get(mask uint32, offset uint32) uint32 {
	return (this.data & mask) >> offset
}
