package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestSchemePgSql
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"testing"
)

func TestTrimVersion(t *testing.T) {
	fmt.Println(trimVersion("table_0"))

	fmt.Println(trimVersion("_table_1_0"))

	fmt.Println(trimVersion("_table_1__0"))

	fmt.Println(trimVersion("_0"))

	fmt.Println(trimVersion("_a"))

}

func TestSchemePgSql(t *testing.T) {
	dbc, err := SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")

	err = dropTablePgSql(dbc, &db.TableDef{
		Name:      "test",
		DbVersion: 0,
	})

	fmt.Println("dropTablePgSql", err)

	t1 := db.TableDef{
		Name:      "test",
		DbVersion: 0,
	}

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field1",
		Type:         "int",
		DefaultValue: "1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field2",
		Type:         "float",
		DefaultValue: "1.1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field3",
		Type:         "string",
		DefaultValue: "hello",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field4",
		Type:         "blob",
		DefaultValue: "",
	})

	err = createTablesPgSql(dbc, &t1)

	fmt.Println("createTablesPgSql", err)

	err = createTablesPgSql(dbc, &t1, &db.TableDef{
		Name:      "test2",
		DbVersion: 0,
	})

	fmt.Println("createTablesPgSql", err)

	_, err = dbc.Exec("insert into test_0 (__key__) values('key');")

	fmt.Println(err)

	rows, err := dbc.Query("select field4_0 from test_0 where __key__='key';")

	fmt.Println(err)

	if rows.Next() {

		var field4 []byte

		err = rows.Scan(&field4)

		fmt.Println(err, len(field4))
	}

	t2 := db.TableDef{
		Name:      "test",
		DbVersion: 0,
	}

	t2.Fields = append(t2.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field5",
		Type:         "int",
		DefaultValue: "1",
	})

	t2.Fields = append(t2.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field6",
		Type:         "float",
		DefaultValue: "1.1",
	})

	err = addFieldsPgSql(dbc, &t2)

	fmt.Println("alterTablePgSql", err)

	d, err := getTableSchemePgSql(dbc, "test_0")
	fmt.Println(d, err)

	for _, v := range d.Fields {
		fmt.Println(v)
	}

}

func TestSchemeMySql(t *testing.T) {
	dbc, err := SqlOpen("mysql", "localhost", 3306, "test", "root", "12345678")

	err = dropTableMySql(dbc, &db.TableDef{
		Name:      "test",
		DbVersion: 0,
	})

	fmt.Println("dropTableMySql", err)

	t1 := db.TableDef{
		Name:      "test",
		DbVersion: 0,
	}

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field1",
		Type:         "int",
		DefaultValue: "1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field2",
		Type:         "float",
		DefaultValue: "1.1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field3",
		Type:         "string",
		DefaultValue: "hello",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field4",
		Type:         "blob",
		DefaultValue: "",
	})

	err = createTablesMySql(dbc, &t1)

	fmt.Println("createTablesMySql2", err)

	err = createTablesMySql(dbc, &t1, &db.TableDef{
		Name:      "test2",
		DbVersion: 0,
	})

	_, err = dbc.Exec("insert into test_0 (__key__) values('key');")

	fmt.Println(err)

	rows, err := dbc.Query("select field4_0 from test_0 where __key__='key';")

	fmt.Println(err)

	if rows.Next() {

		var field4 []byte

		err = rows.Scan(&field4)

		fmt.Println(err, len(field4))
	}

	t2 := db.TableDef{
		Name:      "test",
		DbVersion: 0,
	}

	t2.Fields = append(t2.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field5",
		Type:         "int",
		DefaultValue: "1",
	})

	t2.Fields = append(t2.Fields, &db.FieldDef{
		TabVersion:   0,
		Name:         "field6",
		Type:         "float",
		DefaultValue: "1.1",
	})

	err = addFieldsMySql(dbc, &t2)

	fmt.Println("alterTableMySql", err)

	d, err := getTableSchemeMySql(dbc, "test_0")
	fmt.Println(d, err)

	for _, v := range d.Fields {
		fmt.Println(v)
	}

}
