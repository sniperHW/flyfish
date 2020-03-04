package str

import (
	//"fmt"
	"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStr(t *testing.T) {

	//force str to expand
	strThreshold = 32

	str := Get()

	str.AppendInt64(100)

	assert.Equal(t, str.Len(), 8)

	i64, _, err := str.ReadInt64(0)

	assert.Equal(t, i64, int64(100))

	f1 := proto.PackField("int", 100)
	f2 := proto.PackField("uint", uint64(200))
	f3 := proto.PackField("float", 0.1)
	f4 := proto.PackField("string", "hello")
	f5 := proto.PackField("blog", []byte("world"))

	str = Get()

	str.AppendField(f1).AppendField(f2).AppendField(f3).AppendField(f4).AppendField(f5)

	offset := 0

	f1, offset, err = str.ReadField(offset)

	assert.Nil(t, err)

	assert.Equal(t, f1.GetInt(), int64(100))

	f2, offset, err = str.ReadField(offset)

	assert.Nil(t, err)

	assert.Equal(t, f2.GetUint(), uint64(200))

	f3, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f3)

	assert.Equal(t, f3.GetFloat(), float64(0.1))

	f4, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f4)
	assert.Equal(t, f4.GetString(), "hello")

	f5, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f5)
	assert.Equal(t, string(f5.GetBlob()), "world")

	str.AppendInt32(32)

	offset = str.Len() - 4

	f6, offset, err := str.ReadInt32(offset)

	assert.Nil(t, err)
	assert.Equal(t, f6, int32(32))

	offset = str.Len() - 4
	str.SetInt32(offset, 64)
	f6, offset, err = str.ReadInt32(offset)

	assert.Nil(t, err)
	assert.Equal(t, f6, int32(64))

	_, _, err = str.ReadInt32(offset)
	assert.NotNil(t, err)

	str = NewStr(str.Bytes(), str.Len())

	offset = 0

	f1, offset, err = str.ReadField(offset)

	assert.Nil(t, err)

	assert.Equal(t, f1.GetInt(), int64(100))

	f2, offset, err = str.ReadField(offset)

	assert.Nil(t, err)

	assert.Equal(t, f2.GetUint(), uint64(200))

	f3, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f3)

	assert.Equal(t, f3.GetFloat(), float64(0.1))

	f4, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f4)
	assert.Equal(t, f4.GetString(), "hello")

	f5, offset, err = str.ReadField(offset)

	assert.Nil(t, err)
	assert.NotNil(t, f5)
	assert.Equal(t, string(f5.GetBlob()), "world")

	{
		str := Get()
		str_a := Get()
		str_b := Get()
		str_c := Get()

		str_a.AppendString("a")
		str_b.AppendString("b")
		str_c.AppendString("c")

		str.Join(",", str_a, str_b, str_c)

		assert.Equal(t, string(str.Bytes()), "a,b,c")

	}

	{
		str := Get()

		f := proto.PackField("blog", []byte("world"))

		str.AppendFieldStr(f, func(s *Str, b []byte) {
			s.AppendString("this is blob")
		})

		s, _, err := str.ReadString(0, len(string("this is blob")))
		assert.Nil(t, err)
		assert.Equal(t, s, "this is blob")

		str.Reset()
		assert.Equal(t, str.Len(), 0)
		Put(str)

	}

}
