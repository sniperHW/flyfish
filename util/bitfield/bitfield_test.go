package bitfield

import (
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
		if nil == b32 {
			t.Fatal("failed11")
		}

		if nil != b32.Set(field_status, cache_ok) {
			t.Fatal("failed12")
		}

		if status, err := b32.Get(field_status); nil != err || status != cache_ok {
			t.Fatal("failed13")
		}

		if nil != b32.Set(field_status, cache_missing) {
			t.Fatal("failed14")
		}

		if status, err := b32.Get(field_status); nil != err || status != cache_missing {
			t.Fatal("failed15")
		}

		if nil != b32.Set(field_sql_flag, sql_update) {
			t.Fatal("failed16")
		}

		if sqlflag, err := b32.Get(field_sql_flag); nil != err || sqlflag != sql_update {
			t.Fatal("failed17")
		}

		if status, err := b32.Get(field_status); nil != err || status != cache_missing {
			t.Fatal("failed18")
		}

	}

	{
		b32 := NewBitField32(field_status, MakeFiled32(0x7))
		if nil != b32 {
			t.Fatal("failed21")
		}

		b32 = NewBitField32(field_status)
		if nil == b32 {
			t.Fatal("failed22")
		}

		if nil == b32.Set(field_sql_flag, sql_update) {
			t.Fatal("failed32")
		}
	}

}
