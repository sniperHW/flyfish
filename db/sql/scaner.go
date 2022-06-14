package sql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/proto"
	"strings"
)

const selectTemplate string = "select %s from %s where __slot__ = %d and __version__ > 0 and __key__ not in ('%s');"

type ScannerRow struct {
	Key     string
	Version int64
	Fields  []*proto.Field //字段
}

type Scanner struct {
	wantFields          []string
	field_value_convter []func(interface{}) interface{}
	field_value_creator []func() interface{}
	rows                *sql.Rows
}

func NewScanner(tbmeta db.TableMeta, dbc *sqlx.DB, slot int, wantFields []string, exclude []string) (*Scanner, error) {

	queryFields := []string{"__key__", "__version__"}
	for _, v := range wantFields {
		queryFields = append(queryFields, tbmeta.(*TableMeta).GetRealFieldName(v))
	}

	sqlStr := fmt.Sprintf(selectTemplate, strings.Join(queryFields, ","), tbmeta.(*TableMeta).real_tableName, slot, strings.Join(exclude, "','"))

	rows, err := dbc.Query(sqlStr)

	if nil != err {
		return nil, err
	}

	scaner := &Scanner{
		wantFields: wantFields,
		rows:       rows,
	}

	scaner.field_value_creator = append(scaner.field_value_creator, proto.ValueReceiverFactory(proto.ValueType_string)) //__key__
	scaner.field_value_creator = append(scaner.field_value_creator, proto.ValueReceiverFactory(proto.ValueType_int))    //__version__

	fieldMetas := tbmeta.(*TableMeta).fieldMetas

	for _, v := range wantFields {
		fieldMeta := fieldMetas[v]
		scaner.field_value_creator = append(scaner.field_value_creator, proto.ValueReceiverFactory(fieldMeta.tt))
		scaner.field_value_convter = append(scaner.field_value_convter, proto.GetValueConvertor(fieldMeta.tt))
	}

	return scaner, nil
}

func (sc *Scanner) makeFieldReceiver() (receiver []interface{}) {
	for _, v := range sc.field_value_creator {
		receiver = append(receiver, v())
	}
	return
}

func (sc *Scanner) Next(count int) (rows []*ScannerRow, err error) {
	if count > 0 {
		for sc.rows.Next() {
			field_receiver := sc.makeFieldReceiver()

			err = sc.rows.Scan(field_receiver...)
			if err != nil {
				return
			}

			fields := []*proto.Field{}
			for i := 0; i < len(sc.wantFields); i++ {
				fields = append(fields, proto.PackField(sc.wantFields[i], sc.field_value_convter[i](field_receiver[i+2])))
			}
			rows = append(rows, &ScannerRow{
				Key:     *field_receiver[0].(*string),
				Version: *field_receiver[1].(*int64),
				Fields:  fields,
			})

			count--

			if count == 0 {
				break
			}
		}
	}
	return
}

func (sc *Scanner) Close() {
	sc.rows.Close()
}
