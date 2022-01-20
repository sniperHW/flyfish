package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestScaner
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScaner(t *testing.T) {
	dbdef := &db.DbDef{}
	tdef := &db.TableDef{
		Name: "users1",
	}

	tdef.Fields = append(tdef.Fields, &db.FieldDef{
		Name:        "name",
		Type:        "string",
		DefautValue: "",
	})

	tdef.Fields = append(tdef.Fields, &db.FieldDef{
		Name:        "age",
		Type:        "int",
		DefautValue: "1",
	})

	tdef.Fields = append(tdef.Fields, &db.FieldDef{
		Name:        "phone",
		Type:        "string",
		DefautValue: "",
	})

	dbdef.TableDefs = append(dbdef.TableDefs, tdef)

	meta, err := CreateDbMeta(dbdef)
	assert.Nil(t, err)
	dbc, err := SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")
	assert.Nil(t, err)

	scaner, err := NewScanner(meta.GetTableMeta("users1"), dbc, 2, []string{"name"}, []string{"huangwei:3", "sniper"})
	assert.Nil(t, err)

	count := 0

	for {
		rows, err := scaner.Next(10)
		assert.Nil(t, err)
		if len(rows) == 0 {
			break
		}
		count += len(rows)
		for _, v := range rows {
			fmt.Println(v.Key, v.Version, v.Fields)
		}
	}

	fmt.Println(count)

}
