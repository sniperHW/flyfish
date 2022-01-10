package bitmap

import (
	"encoding/json"
)

type Bitmap struct {
	max      int
	bits     []byte
	dirty    bool
	openBits []int
}

func New(max int) *Bitmap {
	var l int
	l = max >> 3
	if max%8 > 0 {
		l++
	}

	return &Bitmap{
		max:   max,
		bits:  make([]byte, l, l),
		dirty: true,
	}
}

func (b *Bitmap) Clone() *Bitmap {
	o := &Bitmap{
		max:   b.max,
		bits:  make([]byte, len(b.bits), len(b.bits)),
		dirty: true,
	}
	copy(o.bits, b.bits)
	return o
}

func (b *Bitmap) GetOpenBits() []int {
	if b.dirty {
		b.dirty = false
		b.openBits = b.openBits[:0]
		for i := 0; i < len(b.bits); i++ {
			v := b.bits[i]
			if v > 0 {
				for j := 0; j < 8; j++ {
					if v&(1<<j) > 0 {
						b.openBits = append(b.openBits, i*8+j)
					}
				}
			}
		}
	}
	return b.openBits
}

func (b *Bitmap) Set(i int, o ...int) {
	b.set(i)
	for _, v := range o {
		b.set(v)
	}
}

func (b *Bitmap) set(i int) {
	if i < b.max {
		k := i >> 3
		j := i % 8
		b.bits[k] = b.bits[k] | (1 << j)
		b.dirty = true
	}
}

func (b *Bitmap) Clear(i int, o ...int) {
	b.clear(i)
	for _, v := range o {
		b.clear(v)
	}
}

func (b *Bitmap) clear(i int) {
	if i < b.max {
		k := i >> 3
		j := i % 8
		b.bits[k] = b.bits[k] & (0xFF ^ (1 << j))
		b.dirty = true
	}
}

func (b *Bitmap) Test(i int) bool {
	if i < b.max {
		k := i >> 3
		j := i % 8
		return b.bits[k]&(1<<j) > 0

	} else {
		return false
	}
}

func (b *Bitmap) Bytes() []byte {
	return b.bits
}

type jsonBitmap struct {
	Max  int
	Bits []byte
}

func (b *Bitmap) ToJson() []byte {
	j := jsonBitmap{
		Max:  b.max,
		Bits: b.bits,
	}

	bytes, _ := json.Marshal(j)
	return bytes
}

func CreateFromJson(b []byte) (*Bitmap, error) {
	j := jsonBitmap{}
	err := json.Unmarshal(b, &j)
	if nil != err {
		return nil, err
	}

	return &Bitmap{
		max:   j.Max,
		dirty: true,
		bits:  j.Bits,
	}, nil
}
