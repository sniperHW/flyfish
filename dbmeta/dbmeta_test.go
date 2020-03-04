package dbmeta

import (
	"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadDef(t *testing.T) {

	defs := []string{
		"users1@age:int:0,phone:string:123,name:string:haha,blob:blob:",
		"role_module_data@guidance:string:,weapon_fetter:string:,attr:string:,equipment:string:",
	}

	meta, _ := NewDBMeta(defs)

	assert.NotNil(t, meta)

	meta.Reload(defs)

	assert.Equal(t, meta.CheckMetaVersion(int64(2)), true)

	users1_meta := meta.GetTableMeta("users1")

	assert.NotNil(t, users1_meta)

	age_meta, ok := users1_meta.fieldMetas["age"]

	assert.Equal(t, ok, true)

	assert.Equal(t, age_meta.tt, proto.ValueType_int)

	assert.Nil(t, meta.GetTableMeta("users2"))

}
