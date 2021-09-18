package flykv

//go test -run=TestDBTable

import (
	"fmt"
	//"github.com/sniperHW/flyfish/backend/db/sql"
	//"github.com/stretchr/testify/assert"
	"github.com/sniperHW/flyfish/backend/db"
	"testing"
)

func TestDBTable(t *testing.T) {
	m := db.DbDef{}

	t1 := db.TableDef{
		Name: "users1",
	}

	field1 := db.FieldDef{
		Name:        "name",
		Type:        "string",
		DefautValue: "",
	}

	field2 := db.FieldDef{
		Name:        "age",
		Type:        "int",
		DefautValue: "0",
	}

	field3 := db.FieldDef{
		Name:        "phone",
		Type:        "string",
		DefautValue: "",
	}

	t1.Fields = append(t1.Fields, field1)
	t1.Fields = append(t1.Fields, field2)
	t1.Fields = append(t1.Fields, field3)

	m.TableDefs = append(m.TableDefs, t1)

	defStr, _ := db.DbDefToJsonString(&m)

	fmt.Println(string(defStr))

}
