package bitfield

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	cache_new     = uint32(1) //正在从数据库加载
	cache_ok      = uint32(2) //
	cache_missing = uint32(3)
	cache_remove  = uint32(4)
)

const (
	sql_none          = uint32(0)
	sql_insert_update = uint32(1)
	sql_update        = uint32(2)
	sql_delete        = uint32(3)
)

var (
	field_status     = MakeFiled32(0xF)
	field_sql_flag   = MakeFiled32(0xF0)
	field_writeback  = MakeFiled32(0xF00)
	field_snapshoted = MakeFiled32(0xF000)
	field_tmp        = MakeFiled32(0xF0000)
	field_kicking    = MakeFiled32(0xF00000)
)

func TestBitField(t *testing.T) {
	{
		b32 := NewBitField32(field_status, field_sql_flag, field_writeback, field_snapshoted, field_tmp, field_kicking)
		assert.NotNil(t, b32)

		assert.Nil(t, b32.Set(field_status, cache_ok))

		var status uint32
		var sqlflag uint32
		var err error

		status, err = b32.Get(field_status)

		assert.Nil(t, err)
		assert.Equal(t, status, cache_ok)

		assert.Nil(t, b32.Set(field_status, cache_missing))

		status, err = b32.Get(field_status)

		assert.Nil(t, err)
		assert.Equal(t, status, cache_missing)

		assert.Nil(t, b32.Set(field_sql_flag, sql_update))

		sqlflag, err = b32.Get(field_sql_flag)

		assert.Nil(t, err)
		assert.Equal(t, sqlflag, sql_update)

		status, err = b32.Get(field_status)

		assert.Nil(t, err)
		assert.Equal(t, status, cache_missing)

	}

	{
		b32 := NewBitField32(field_status, MakeFiled32(0x7))
		assert.Nil(t, b32)
		b32 = NewBitField32(field_status)
		assert.NotNil(t, b32)

		assert.NotNil(t, b32.Set(field_sql_flag, sql_update))
	}

}
