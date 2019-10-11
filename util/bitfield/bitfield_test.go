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

const (
	kv_status_offset    = uint32(0)
	mask_kv_status      = uint32(0xF << kv_status_offset) //1-4位kv状态
	kv_sql_flag_offset  = uint32(4)
	mask_kv_sql_flag    = uint32(0xF << kv_sql_flag_offset) //5-8位sql回写标记
	kv_writeback_offset = uint32(8)
	mask_kv_writeback   = uint32(0xF << kv_writeback_offset) //9-12位当前是否正在执行sql回写
	//kv_snapshoted_offset = uint32(12)
	//mask_kv_snapshoted   = uint32(0xF << kv_snapshoted_offset) //13-16位是否已经建立过快照
	kv_tmp_offset     = uint32(16)
	mask_kv_tmp       = uint32(0xF << kv_tmp_offset) //17-20位,是否临时kv
	kv_kicking_offset = uint32(20)
	mask_kv_kicking   = uint32(0xF << kv_kicking_offset) //21-24位,是否正在被踢除
)

func TestBitField(t *testing.T) {
	var b32 BitField32

	b32.Set(mask_kv_status, kv_status_offset, cache_new)

	b32.Set(mask_kv_status, kv_status_offset, cache_ok)

	if cache_ok != b32.Get(mask_kv_status, kv_status_offset) {
		t.Fatal("failed1")
	}

	b32.Set(mask_kv_sql_flag, kv_sql_flag_offset, sql_delete)

	if sql_delete != b32.Get(mask_kv_sql_flag, kv_sql_flag_offset) {
		t.Fatal("failed2")
	}

	b32.Set(mask_kv_sql_flag, kv_sql_flag_offset, sql_update)

	if sql_update != b32.Get(mask_kv_sql_flag, kv_sql_flag_offset) {
		t.Fatal("failed3")
	}

	if cache_ok != b32.Get(mask_kv_status, kv_status_offset) {
		t.Fatal("failed4")
	}

	/*b32.Set(mask_kv_tmp, kv_tmp_offset, uint32(1))

	if uint32(1) != b32.Get(mask_kv_tmp, kv_tmp_offset) {
		t.Fatal("failed1", b32.data)
	}

	b32.Set(mask_kv_status, kv_status_offset, uint32(3))

	if uint32(3) != b32.Get(mask_kv_status, kv_status_offset) {
		t.Fatal("failed2", b32.data)
	}*/

}
