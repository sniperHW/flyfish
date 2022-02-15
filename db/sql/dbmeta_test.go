package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/logger"
	//"github.com/sniperHW/flyfish/pkg/buffer"
	//"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func init() {
	InitLogger(logger.NewZapLogger("sql.log", "./log", "debug", 4096, 1, 1, true))
}

func TestDbmeta1(t *testing.T) {
	m := db.DbDef{
		Version: 1,
	}

	t1 := db.TableDef{
		DbVersion: 3,
		Version:   1,
		Name:      "Table1",
	}

	{
		field1 := db.FieldDef{
			TabVersion:   4,
			Name:         "field1",
			Type:         "int",
			DefaultValue: "1",
		}

		field2 := db.FieldDef{
			TabVersion:   4,
			Name:         "field2",
			Type:         "float",
			DefaultValue: "1.2",
		}

		field3 := db.FieldDef{
			TabVersion:   3,
			Name:         "field3",
			Type:         "string",
			DefaultValue: "hello",
		}

		field4 := db.FieldDef{
			TabVersion: 2,
			Name:       "field4",
			Type:       "string",
		}

		field5 := db.FieldDef{
			TabVersion: 1,
			Name:       "field5",
			Type:       "blob",
		}

		t1.Fields = append(t1.Fields, &field1)
		t1.Fields = append(t1.Fields, &field2)
		t1.Fields = append(t1.Fields, &field3)
		t1.Fields = append(t1.Fields, &field4)
		t1.Fields = append(t1.Fields, &field5)
	}

	m.TableDefs = append(m.TableDefs, &t1)

	//defStr, err := db.DbDefToJsonString(&m)
	//assert.Nil(t, err)

	prettyJSON, _ := json.MarshalIndent(&m, "", "    ")

	fmt.Println(string(prettyJSON))

	mt, _ := CreateDbMeta(&m)

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

	//testSqlString(t, td)

}

/*
func testSqlString(t *testing.T, meta db.TableMeta) {
	us := &db.UpdateState{
		Version: 1,
		Key:     "hello",
		Slot:    1,
		Fields:  map[string]*proto.Field{},
		Meta:    meta,
	}

	us.Fields["field1"] = proto.PackField("field1", 1)
	us.Fields["field2"] = proto.PackField("field2", 1.2)

	pg := &sqlstring{
		binarytostr: binaryTopgSqlStr,
	}

	pg.buildInsertUpdateString = pg.insertUpdateStatementPgSql

	b := buffer.New()
	pg.insertStatement(b, us)

	fmt.Println(string(b.Bytes()))

	b = buffer.New()

	pg.deleteStatement(b, us)
	fmt.Println(string(b.Bytes()))

	b = buffer.New()

	pg.updateStatement(b, us)
	fmt.Println(string(b.Bytes()))

	b = buffer.New()

	pg.insertUpdateStatement(b, us)
	fmt.Println(string(b.Bytes()))

	mysql := &sqlstring{
		binarytostr: binaryTomySqlStr,
	}

	mysql.buildInsertUpdateString = mysql.insertUpdateStatementMySql

	b = buffer.New()

	mysql.insertUpdateStatement(b, us)
	fmt.Println(string(b.Bytes()))

}
*/
