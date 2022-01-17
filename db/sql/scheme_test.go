package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestScheme
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"testing"
)

func TestScheme(t *testing.T) {
	dbc, err := SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")

	err = dropTablePgSql(dbc, &db.TableDef{
		Name:      "test",
		DbVersion: 0,
	})

	fmt.Println(err)

	t1 := db.TableDef{
		Name:      "test",
		DbVersion: 0,
	}

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:  0,
		Name:        "field1",
		Type:        "int",
		DefautValue: "1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:  0,
		Name:        "field2",
		Type:        "float",
		DefautValue: "1.1",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:  0,
		Name:        "field3",
		Type:        "string",
		StrCap:      1024,
		DefautValue: "hello",
	})

	t1.Fields = append(t1.Fields, &db.FieldDef{
		TabVersion:  0,
		Name:        "field4",
		Type:        "blob",
		DefautValue: "",
	})

	err = createTablesPgSql(dbc, &t1)

	fmt.Println(err)

	err = createTablesPgSql(dbc, &t1, &db.TableDef{
		Name:      "test2",
		DbVersion: 0,
	})

	fmt.Println(err)

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
		TabVersion:  0,
		Name:        "field5",
		Type:        "int",
		DefautValue: "1",
	})

	t2.Fields = append(t2.Fields, &db.FieldDef{
		TabVersion:  0,
		Name:        "field6",
		Type:        "float",
		DefautValue: "1.1",
	})

	err = alterTablePgSql(dbc, &t2)

	fmt.Println(err)

}
