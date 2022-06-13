package flysql

//go test -race -covermode=atomic -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"context"
	//"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	//"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"testing"
)

func test(t *testing.T, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta) {
	//delete

	version, err := MarkDelete(context.TODO(), dbc, dbtype, tbmeta, "hw", sslot.Unikey2Slot("users1:hw"))

	assert.Nil(t, err)
	assert.Equal(t, version, int64(-1))

	//已经被标记删除
	version, err = MarkDelete(context.TODO(), dbc, dbtype, tbmeta, "hw", sslot.Unikey2Slot("users1:hw"))
	assert.Nil(t, err)
	assert.Equal(t, version, int64(-1))

	fields := map[string]*proto.Field{}
	fields["name"] = proto.PackField("name", "hw1")
	fields["age"] = proto.PackField("age", 2)

	//set

	version, err = Set(context.TODO(), dbc, dbtype, tbmeta, "hw1", sslot.Unikey2Slot("users1:hw1"), fields)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(1))

	fields["age"] = proto.PackField("age", 3)
	version, err = Set(context.TODO(), dbc, dbtype, tbmeta, "hw1", sslot.Unikey2Slot("users1:hw1"), fields)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(2))

	//版本号不匹配,不变更
	version, err = Set(context.TODO(), dbc, dbtype, tbmeta, "hw1", sslot.Unikey2Slot("users1:hw1"), fields, 1)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(2))

	//版本号匹配，变更，返回的版本号绝对值+1
	fields["age"] = proto.PackField("age", 4)
	version, err = Set(context.TODO(), dbc, dbtype, tbmeta, "hw1", sslot.Unikey2Slot("users1:hw1"), fields, 2)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(3))

	//版本号匹配，变更，返回的版本号绝对值+1
	fields["age"] = proto.PackField("age", 5)
	version, err = Set(context.TODO(), dbc, dbtype, tbmeta, "hw1", sslot.Unikey2Slot("users1:hw1"), fields, 3)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(4))

	fields["name"] = proto.PackField("name", "hw2")
	fields["age"] = proto.PackField("age", 2)

	//setNx

	version, retFields, err := SetNx(context.TODO(), dbc, dbtype, tbmeta, "hw2", sslot.Unikey2Slot("users1:hw2"), fields)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(1))
	assert.Nil(t, retFields)

	version, retFields, err = SetNx(context.TODO(), dbc, dbtype, tbmeta, "hw2", sslot.Unikey2Slot("users1:hw2"), fields)
	assert.Nil(t, err)
	assert.Equal(t, version, int64(1))
	assert.Equal(t, retFields["age"].GetInt(), int64(2))

	//compareAndSet

	version, _, err = CompareAndSet(context.TODO(), dbc, dbtype, tbmeta,
		"hw3", sslot.Unikey2Slot("users1:hw3"), proto.PackField("age", 1), proto.PackField("age", 2))

	assert.Equal(t, err, ErrRecordNotExist)

	version, retField, err := CompareAndSet(context.TODO(), dbc, dbtype, tbmeta,
		"hw2", sslot.Unikey2Slot("users1:hw2"), proto.PackField("age", 2), proto.PackField("age", 3))

	assert.Nil(t, err)
	assert.Equal(t, version, int64(2))
	assert.Equal(t, retField.GetInt(), int64(3))

	//compareAndSetNx
	version, retField, err = CompareAndSetNx(context.TODO(), dbc, dbtype, tbmeta,
		"hw4", sslot.Unikey2Slot("users1:hw4"), proto.PackField("age", 1), proto.PackField("age", 2))
	assert.Nil(t, err)
	assert.Equal(t, version, int64(1))
	assert.Equal(t, retField.GetInt(), int64(2))

	//compare不匹配
	version, retField, err = CompareAndSetNx(context.TODO(), dbc, dbtype, tbmeta,
		"hw4", sslot.Unikey2Slot("users1:hw4"), proto.PackField("age", 1), proto.PackField("age", 3))
	assert.Equal(t, err, ErrCompareNotEqual)
	assert.Equal(t, version, int64(1))
	assert.Equal(t, retField.GetInt(), int64(2))

	//compare匹配
	version, retField, err = CompareAndSetNx(context.TODO(), dbc, dbtype, tbmeta,
		"hw4", sslot.Unikey2Slot("users1:hw4"), proto.PackField("age", 2), proto.PackField("age", 3))
	assert.Nil(t, err)
	assert.Equal(t, version, int64(2))
	assert.Equal(t, retField.GetInt(), int64(3))

	//Add
	version, retField, err = Add(context.TODO(), dbc, dbtype, tbmeta,
		"hw5", sslot.Unikey2Slot("users1:hw5"), proto.PackField("age", 5))
	assert.Nil(t, err)
	assert.Equal(t, version, int64(1))
	assert.Equal(t, retField.GetInt(), int64(5))

	version, retField, err = Add(context.TODO(), dbc, dbtype, tbmeta,
		"hw5", sslot.Unikey2Slot("users1:hw5"), proto.PackField("age", -2))
	assert.Nil(t, err)
	assert.Equal(t, version, int64(2))
	assert.Equal(t, retField.GetInt(), int64(3))

	{
		version, retFields, err := Load(context.TODO(), dbc, tbmeta, "hw5", []string{"age"})
		assert.Nil(t, err)
		assert.Equal(t, version, int64(2))
		assert.Equal(t, retFields[0].GetInt(), int64(3))
	}

	{
		_, _, err := Load(context.TODO(), dbc, tbmeta, "hw6", []string{"age"}, 1)
		assert.Equal(t, err, ErrRecordNotExist)

		_, _, err = Load(context.TODO(), dbc, tbmeta, "hw5", []string{"age"}, 2)
		assert.Equal(t, err, ErrRecordNotChange)

		version, retFields, err := Load(context.TODO(), dbc, tbmeta, "hw5", []string{"age"}, 1)
		assert.Nil(t, err)
		assert.Equal(t, version, int64(2))
		assert.Equal(t, retFields[0].GetInt(), int64(3))

	}

}

func TestPgsql(t *testing.T) {

	metaStr := `
{
	"TableDefs":[
	{"Name":"users1",
	 "Fields":[
	 	{"Name":"name","Type":"string","DefaultValue":""},
	 	{"Name":"age","Type":"int","DefaultValue":"0"},
	 	{"Name":"phone","Type":"blob","DefaultValue":""}]
	}]
}

	`

	dbdef, err := db.MakeDbDefFromJsonString([]byte(metaStr))
	if nil != err {
		panic(err)
	}

	meta, err := sql.CreateDbMeta(dbdef)

	if nil != err {
		panic(err)
	}

	dbc, err := sql.SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")
	defer dbc.Close()
	if nil != err {
		panic(err)
	}

	sql.ClearAllTableData(dbc, meta)

	test(t, dbc, "pgsql", meta.GetTableMeta("users1").(*sql.TableMeta))

}

func TestMysql(t *testing.T) {

	metaStr := `
{
	"TableDefs":[
	{"Name":"users1",
	 "Fields":[
	 	{"Name":"name","Type":"string","DefaultValue":""},
	 	{"Name":"age","Type":"int","DefaultValue":"0"},
	 	{"Name":"phone","Type":"blob","DefaultValue":""}]
	}]
}

	`

	dbdef, err := db.MakeDbDefFromJsonString([]byte(metaStr))
	if nil != err {
		panic(err)
	}

	meta, err := sql.CreateDbMeta(dbdef)

	if nil != err {
		panic(err)
	}

	dbc, err := sql.SqlOpen("mysql", "localhost", 3306, "test", "sniper", "802802")
	defer dbc.Close()
	if nil != err {
		panic(err)
	}

	sql.ClearAllTableData(dbc, meta)

	test(t, dbc, "mysql", meta.GetTableMeta("users1").(*sql.TableMeta))

}
