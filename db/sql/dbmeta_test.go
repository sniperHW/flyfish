package sql

//go test -tags=aio -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestDbmeta1(t *testing.T) {
	m := db.DbDef{}

	t1 := db.TableDef{
		Name: "Table1",
	}

	{
		field1 := db.FieldDef{
			Name:        "field1",
			Type:        "int",
			DefautValue: "1",
		}

		field2 := db.FieldDef{
			Name:        "field2",
			Type:        "float",
			DefautValue: "1.2",
		}

		field3 := db.FieldDef{
			Name:        "field3",
			Type:        "string",
			DefautValue: "hello",
		}

		field4 := db.FieldDef{
			Name: "field4",
			Type: "string",
		}

		field5 := db.FieldDef{
			Name: "field5",
			Type: "blob",
		}

		t1.Fields = append(t1.Fields, &field1)
		t1.Fields = append(t1.Fields, &field2)
		t1.Fields = append(t1.Fields, &field3)
		t1.Fields = append(t1.Fields, &field4)
		t1.Fields = append(t1.Fields, &field5)
	}

	m.TableDefs = append(m.TableDefs, &t1)

	defStr, err := db.DbDefToJsonString(&m)
	assert.Nil(t, err)
	fmt.Println(string(defStr))

	mt := CreateDbMeta(1, &m)

	td := mt.GetTableMeta("Table1").(*TableMeta)

	fmt.Println(td.GetQueryMeta().GetFieldNames())

	r := td.GetQueryMeta().GetReceiver()

	assert.Equal(t, reflect.TypeOf(r[0]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[1]).String(), "*int64")
	assert.Equal(t, reflect.TypeOf(r[3]).String(), "*int64")
	assert.Equal(t, reflect.TypeOf(r[4]).String(), "*float64")
	assert.Equal(t, reflect.TypeOf(r[5]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[6]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[7]).String(), "*[]uint8")

	assert.Equal(t, td.GetDefaultValue("field1"), int64(1))
	assert.Equal(t, td.GetDefaultValue("field2"), float64(1.2))
	assert.Equal(t, td.GetDefaultValue("field3"), "hello")
	assert.Equal(t, td.GetDefaultValue("field4"), "")
	assert.Equal(t, td.GetDefaultValue("field5"), []byte{})

	{
		assert.Equal(t, db.GetDefaultValue(proto.ValueType_int, ""), int64(0))
		assert.Equal(t, db.GetDefaultValue(proto.ValueType_float, ""), float64(0))
	}

	{
		s := "hello"
		assert.Equal(t, td.GetQueryMeta().GetConvetorByName("field4")(&s), "hello")
		i := int64(1)
		assert.Equal(t, convert_int64(&i), int64(1))
		f := float64(1.2)
		assert.Equal(t, convert_float(&f), float64(1.2))

		b := []byte("string")
		assert.Equal(t, convert_blob(&b), []byte("string"))
	}

	fmt.Println(td.GetInsertPrefix())
	fmt.Println(td.GetSelectPrefix())

}

func TestDbmeta2(t *testing.T) {
	dbdef, err := db.CreateDbDefFromCsv([]string{"Table1@field1:int:1,field2:float:1.2,field3:string:hello,field4:string:,field5:blob:"})

	assert.Nil(t, err)

	mt := CreateDbMeta(1, dbdef)

	defStr, err := db.DbDefToJsonString(dbdef)
	assert.Nil(t, err)
	fmt.Println(string(defStr))

	td := mt.GetTableMeta("Table1").(*TableMeta)

	fmt.Println(td.GetQueryMeta().GetFieldNames())

	r := td.GetQueryMeta().GetReceiver()

	assert.Equal(t, reflect.TypeOf(r[0]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[1]).String(), "*int64")
	assert.Equal(t, reflect.TypeOf(r[3]).String(), "*int64")
	assert.Equal(t, reflect.TypeOf(r[4]).String(), "*float64")
	assert.Equal(t, reflect.TypeOf(r[5]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[6]).String(), "*string")
	assert.Equal(t, reflect.TypeOf(r[7]).String(), "*[]uint8")

	assert.Equal(t, td.GetDefaultValue("field1"), int64(1))
	assert.Equal(t, td.GetDefaultValue("field2"), float64(1.2))
	assert.Equal(t, td.GetDefaultValue("field3"), "hello")
	assert.Equal(t, td.GetDefaultValue("field4"), "")
	assert.Equal(t, td.GetDefaultValue("field5"), []byte{})

	{
		assert.Equal(t, db.GetDefaultValue(proto.ValueType_int, ""), int64(0))
		assert.Equal(t, db.GetDefaultValue(proto.ValueType_float, ""), float64(0))
	}

	{
		s := "hello"
		assert.Equal(t, td.GetQueryMeta().GetConvetorByName("field4")(&s), "hello")
		i := int64(1)
		assert.Equal(t, convert_int64(&i), int64(1))
		f := float64(1.2)
		assert.Equal(t, convert_float(&f), float64(1.2))

		b := []byte("string")
		assert.Equal(t, convert_blob(&b), []byte("string"))
	}

	td1 := mt.CheckTableMeta(td)

	assert.Equal(t, td1, td)

	mt.UpdateMeta(2, dbdef)

	td2 := mt.CheckTableMeta(td)

	assert.NotEqual(t, td2, td)

}
