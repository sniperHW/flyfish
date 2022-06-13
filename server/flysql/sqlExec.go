package flysql

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	//"github.com/sniperHW/flyfish/db"
	"context"
	dbsql "database/sql"
	"errors"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/proto"
)

func prepareMarkDeletePgsql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, version ...int64) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		val := tbmeta.GetDefaultValue(name)
		params = append(params, val)
		b.AppendString(fmt.Sprintf("$%d", len(params)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")
	if len(version) > 0 {
		params = append(params, version[0])
		b.AppendString(fmt.Sprintf("__version__ = 0-(abs(%s.__version__)+1) where %s.__key__ = $1 and %s.__version__=$%d;", tbmeta.GetRealTableName(), tbmeta.GetRealTableName(), tbmeta.GetRealTableName(), len(params)))
	} else {
		b.AppendString(fmt.Sprintf("__version__ = 0-(abs(%s.__version__)+1) where %s.__key__ = $1;", tbmeta.GetRealTableName(), tbmeta.GetRealTableName()))
	}

	return b, params
}

func prepareMarkDeleteMysql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, version ...int64) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		val := tbmeta.GetDefaultValue(name)

		params = append(params, val)

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	if len(version) > 0 {
		params = append(params, version[0])
		b.AppendString(fmt.Sprintf("__version__=if(__version__ = ?,0-(abs(__version__)+1),__version__);"))
	} else {
		b.AppendString("__version__ = 0-(abs(__version__)+1);")
	}

	return b, params
}

func MarkDelete(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, version ...int64) (int64, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelSerializable})
	if nil != err {
		return 0, err
	}

	params := []interface{}{key, -1, slot}
	b := buffer.New()

	if dbtype == "mysql" {
		b, params = prepareMarkDeleteMysql(params, b, tbmeta, version...)
	} else {
		b, params = prepareMarkDeletePgsql(params, b, tbmeta, version...)
	}

	_, err = tx.ExecContext(ctx, b.ToStrUnsafe(), params...)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, err
		}
		return 0, err
	}

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__ from %s where __key__ = '%s';", tbmeta.GetRealTableName(), key))

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, err
		}
		return 0, err
	}

	defer rows.Close()

	var v int64

	if rows.Next() {
		err := rows.Scan(&v)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, err
			}
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return v, nil
}

func prepareSetPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field, version ...int64) (*buffer.Buffer, []interface{}) {
	ff := [][]interface{}{}
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		v, ok := setFields[name]
		if !ok {
			val = tbmeta.GetDefaultValue(name)
		} else {
			val = v.GetValue()
			ff = append(ff, []interface{}{tbmeta.GetRealFieldName(name), len(params) + 1})
		}

		params = append(params, val)

		b.AppendString(fmt.Sprintf("$%d", len(params)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")

	for _, v := range ff {
		b.AppendString(fmt.Sprintf(" %s = $%d,", v[0].(string), v[1].(int)))
	}

	if len(version) > 0 {
		params = append(params, version[0])
		b.AppendString(fmt.Sprintf("__version__ = abs(%s.__version__)+1 where %s.__key__ = $1 and %s.__version__=$%d;", tbmeta.GetRealTableName(), tbmeta.GetRealTableName(), tbmeta.GetRealTableName(), len(params)))
	} else {
		b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1 where %s.__key__ = $1;", tbmeta.GetRealTableName(), tbmeta.GetRealTableName()))
	}

	return b, params

}

func prepareSetMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field, version ...int64) (*buffer.Buffer, []interface{}) {
	ff := [][]interface{}{}
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")

	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		v, ok := setFields[name]
		if !ok {
			val = tbmeta.GetDefaultValue(name)
		} else {
			val = v.GetValue()
			ff = append(ff, []interface{}{tbmeta.GetRealFieldName(name), val})
		}

		params = append(params, val)

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	if len(version) > 0 {
		for _, v := range ff {
			b.AppendString(fmt.Sprintf("%s=if(__version__ = ?,?,%s),", v[0].(string), v[0].(string)))
			params = append(params, version[0])
			params = append(params, v[1])
		}
		b.AppendString(" __version__ = if(__version__ = ?, abs(__version__)+1,__version__);")
		params = append(params, version[0])

	} else {
		for _, v := range ff {
			b.AppendString(fmt.Sprintf(" %s = ?,", v[0].(string)))
			params = append(params, v[1])
		}
		b.AppendString(" __version__ = abs(__version__)+1;")
	}
	return b, params
}

func Set(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, fields map[string]*proto.Field, version ...int64) (int64, error) {

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelSerializable})
	if nil != err {
		return 0, err
	}

	params := []interface{}{key, 1, slot}
	b := buffer.New()
	if dbtype == "mysql" {
		b, params = prepareSetMySql(params, b, tbmeta, fields, version...)
	} else {
		b, params = prepareSetPgSql(params, b, tbmeta, fields, version...)
	}

	_, err = tx.ExecContext(ctx, b.ToStrUnsafe(), params...)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, err
		}
		return 0, err
	}

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__ from %s where __key__ = '%s';", tbmeta.GetRealTableName(), key))

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, err
		}
		return 0, err
	}

	defer rows.Close()

	var v int64

	if rows.Next() {
		err := rows.Scan(&v)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, err
			}
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return v, nil

}

func prepareSetNxPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		v, ok := setFields[name]
		if !ok {
			val = tbmeta.GetDefaultValue(name)
		} else {
			val = v.GetValue()
		}

		params = append(params, val)

		b.AppendString(fmt.Sprintf("$%d", len(params)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")

	for i, name := range fields {
		b.AppendString(fmt.Sprintf(" %s = $%d,", tbmeta.GetRealFieldName(name), i+4))
	}

	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1 where %s.__key__ = $1 and %s.__version__ < 0;", tbmeta.GetRealTableName(), tbmeta.GetRealTableName(), tbmeta.GetRealTableName()))

	return b, params

}

func prepareSetNxMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}) {

	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")
	ff := [][]interface{}{}

	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		v, ok := setFields[name]
		if !ok {
			val = tbmeta.GetDefaultValue(name)

		} else {
			val = v.GetValue()
		}

		params = append(params, val)

		ff = append(ff, []interface{}{tbmeta.GetRealFieldName(name), val})

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	for _, v := range ff {
		b.AppendString(fmt.Sprintf("%s=if(__version__ < 0,?,%s),", v[0].(string), v[0].(string)))
		params = append(params, v[1])
	}

	b.AppendString(" __version__ = if(__version__ < 0, abs(__version__)+1,__version__);")
	return b, params
}

func SetNx(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, fields map[string]*proto.Field) (int64, map[string]*proto.Field, error) {

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelSerializable})
	if nil != err {
		return 0, nil, err
	}

	params := []interface{}{key, 1, slot}
	b := buffer.New()
	if dbtype == "mysql" {
		b, params = prepareSetNxMySql(params, b, tbmeta, fields)
	} else {
		b, params = prepareSetNxPgSql(params, b, tbmeta, fields)
	}

	r, err := tx.ExecContext(ctx, b.ToStrUnsafe(), params...)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	}

	rowsAffected, _ := r.RowsAffected()
	var v int64
	var retFields map[string]*proto.Field

	if rowsAffected > 0 {
		//记录不存在只返回版本号
		rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__ from %s where __key__ = '%s';", tbmeta.GetRealTableName(), key))

		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, nil, err
			}
			return 0, nil, err
		}

		defer rows.Close()

		if rows.Next() {
			err := rows.Scan(&v)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return 0, nil, err
				}
				return 0, nil, err
			}
		}

	} else {
		//记录已存在，除了版本号还要返回已经存在的数据
		str := fmt.Sprintf("%s '%s');", tbmeta.GetSelectPrefix(), key)

		rows, err := tx.QueryContext(ctx, str)

		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, nil, err
			}
			return 0, nil, err
		}

		defer rows.Close()

		if rows.Next() {
			queryMeta := tbmeta.GetQueryMeta()
			filed_receiver := queryMeta.GetReceiver()
			field_convter := queryMeta.GetFieldConvter()
			field_names := queryMeta.GetFieldNames()

			err := rows.Scan(filed_receiver...)
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					return 0, nil, err
				}
				return 0, nil, err
			} else {
				retFields = map[string]*proto.Field{}
				v = field_convter[1](filed_receiver[1]).(int64)
				for i := 0; i < len(field_names); i++ {
					name := field_names[i]
					if _, ok := fields[name]; ok {
						retFields[name] = proto.PackField(name, field_convter[i+3](filed_receiver[i+3]))
					}
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}

	return v, retFields, nil
}

var ErrRecordNotExist error = errors.New("record not exist")
var ErrCompareNotEqual error = errors.New("compare not equal")

func CompareAndSet(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, old *proto.Field, new *proto.Field) (int64, *proto.Field, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelSerializable})
	if nil != err {
		return 0, nil, err
	}

	params := []interface{}{new.GetValue(), key, old.GetValue()}
	real_field_name := tbmeta.GetRealFieldName(old.GetName())

	var updateStr string

	if dbtype == "pgsql" {
		updateStr = fmt.Sprintf("update %s set %s = $1 , __version__ = __version__+1 where __key__=$2 and __version__ > 0 and %s = $3;",
			tbmeta.GetRealTableName(), real_field_name, real_field_name)
	} else {
		updateStr = fmt.Sprintf("update %s set %s = ? , __version__ = __version__+1 where __key__=? and __version__ > 0 and %s = ?;",
			tbmeta.GetRealTableName(), real_field_name, real_field_name)
	}

	r, err := tx.ExecContext(ctx, updateStr, params...)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	}

	rowsAffected, _ := r.RowsAffected()
	var v int64
	var retField *proto.Field

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__,%s from %s where __key__ = '%s';", real_field_name, tbmeta.GetRealTableName(), key))

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	}

	defer rows.Close()

	field_receiver := old.GetValueReceiver()
	field_convter := old.GetValueConvtor()

	if rows.Next() {
		err := rows.Scan(&v, field_receiver)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, nil, err
			}
			return 0, nil, err
		} else {
			retField = proto.PackField(old.GetName(), field_convter(field_receiver))
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}

	if rowsAffected > 0 {
		return v, retField, nil
	} else if v != 0 {
		return v, retField, ErrCompareNotEqual
	} else {
		return 0, nil, ErrRecordNotExist
	}
}
