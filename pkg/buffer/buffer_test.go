package buffer

//go test -covermode=count -v -coverprofile=coverage.out
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuffer(t *testing.T) {
	b := Get()

	b.AppendByte('r')
	b.AppendUint16(1)
	b.AppendUint32(2)
	b.AppendUint64(3)

	b.AppendInt16(-1)
	b.AppendInt32(-2)
	b.AppendInt64(-3)
	b.AppendString("hello")

	r := NewReader(b)

	assert.Equal(t, byte('r'), r.GetByte())
	assert.Equal(t, uint16(1), r.GetUint16())
	assert.Equal(t, uint32(2), r.GetUint32())
	assert.Equal(t, uint64(3), r.GetUint64())

	assert.Equal(t, int16(-1), r.GetInt16())
	assert.Equal(t, int32(-2), r.GetInt32())
	assert.Equal(t, int64(-3), r.GetInt64())

	assert.Equal(t, "hello", r.GetString(10))

	assert.Equal(t, len(b.Bytes()), b.Len())

	b = Get()
	b.AppendUint16(1)
	b.AppendUint32(2)
	fmt.Println(b.bs)
	b.DropFirstNBytes(2)
	fmt.Println(b.bs)
	r = NewReader(b)
	assert.Equal(t, uint32(2), r.GetUint32())
	assert.Equal(t, b.Len(), 4)
	b.AppendString("hello")
	b.AppendUint64(3)
	b.DropFirstNBytes(9)
	r = NewReader(b)
	assert.Equal(t, uint64(3), r.GetUint64())
	assert.Equal(t, b.Len(), 8)
	b.DropFirstNBytes(9)
	assert.Equal(t, b.Len(), 8)
	b.DropFirstNBytes(8)
	assert.Equal(t, b.Len(), 0)
}
