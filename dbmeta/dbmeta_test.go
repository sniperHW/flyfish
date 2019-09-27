package dbmeta

import (
	"github.com/sniperHW/flyfish/proto"
	"testing"
)

func TestLoadDef(t *testing.T) {

	defs := []string{
		"users1@age:int:0,phone:string:123,name:string:haha,blob:blob:",
		"role_module_data@guidance:string:,weapon_fetter:string:,attr:string:,equipment:string:",
	}

	meta, _ := NewDBMeta(defs)

	if nil == meta {
		t.Fatal("NewDBMeta failed")
	}

	meta.Reload(defs)

	if meta.version != 2 {
		t.Fatal("Reload failed")
	}

	users1_meta := meta.GetTableMeta("users1")

	if nil == users1_meta {
		t.Fatal("GetTableMeta users1 failed")
	}

	age_meta, ok := users1_meta.fieldMetas["age"]

	if !ok {
		t.Fatal("get age meta failed")
	}

	if age_meta.tt != proto.ValueType_int {
		t.Fatal("age tt error")
	}

	if nil != meta.GetTableMeta("users2") {
		t.Fatal("GetTableMeta users2 should failed")
	}

}
