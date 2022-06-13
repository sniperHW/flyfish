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

func prepareMarkDeletePgsql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	fields := tbmeta.GetAllFieldsName()

	ff := [][]interface{}{}

	for i, name := range fields {
		val := tbmeta.GetDefaultValue(name)
		params = append(params, val)
		b.AppendString(fmt.Sprintf("$%d", len(params)))
		ff = append(ff, []interface{}{tbmeta.GetRealFieldName(name), len(params)})
		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")

	for _, v := range ff {
		b.AppendString(fmt.Sprintf(" %s = $%d,", v[0].(string), v[1].(int)))
	}

	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(fmt.Sprintf("__version__ = 0-(abs(%s.__version__)+1) where %s.__key__ = $1 and %s.__version__ > 0;", real_tab_name, real_tab_name, real_tab_name))
	return b, params
}

func prepareMarkDeleteMysql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")
	fields := tbmeta.GetAllFieldsName()

	ff := [][]interface{}{}

	for i, name := range fields {

		val := tbmeta.GetDefaultValue(name)

		params = append(params, val)

		b.AppendString("?")

		ff = append(ff, []interface{}{tbmeta.GetRealFieldName(name), val})

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	for _, v := range ff {
		b.AppendString(fmt.Sprintf("%s=if(__version__ > 0,?,%s),", v[0].(string), v[0].(string)))
		params = append(params, v[1])
	}
	b.AppendString(" __version__ = if(__version__ > 0, 0-(abs(__version__)+1),__version__);")

	return b, params
}

func MarkDelete(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int) (int64, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if nil != err {
		return 0, err
	}

	params := []interface{}{key, -1, slot}
	b := buffer.New()

	if dbtype == "mysql" {
		b, params = prepareMarkDeleteMysql(params, b, tbmeta)
	} else {
		b, params = prepareMarkDeletePgsql(params, b, tbmeta)
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

	real_tab_name := tbmeta.GetRealTableName()

	if len(version) > 0 {
		params = append(params, version[0])
		b.AppendString(fmt.Sprintf("__version__ = abs(%s.__version__)+1 where %s.__key__ = $1 and %s.__version__=$%d;", real_tab_name, real_tab_name, real_tab_name, len(params)))
	} else {
		b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1 where %s.__key__ = $1;", real_tab_name, real_tab_name))
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

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
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
	ff := [][]interface{}{}

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

	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1 where %s.__key__ = $1 and %s.__version__ < 0;", real_tab_name, real_tab_name, real_tab_name))

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

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
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
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if nil != err {
		return 0, nil, err
	}

	params := []interface{}{new.GetValue(), key, old.GetValue()}
	real_field_name := tbmeta.GetRealFieldName(old.GetName())

	var updateStr string

	real_tab_name := tbmeta.GetRealTableName()

	if dbtype == "pgsql" {
		updateStr = fmt.Sprintf("update %s set %s = $1 , __version__ = __version__+1 where __key__=$2 and __version__ > 0 and %s = $3;",
			real_tab_name, real_field_name, real_field_name)
	} else {
		updateStr = fmt.Sprintf("update %s set %s = ? , __version__ = __version__+1 where __key__=? and __version__ > 0 and %s = ?;",
			real_tab_name, real_field_name, real_field_name)
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

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__,%s from %s where __key__ = '%s';", real_field_name, real_tab_name, key))

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

func prepareCompareAndSetNxPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, old *proto.Field, new *proto.Field) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	var newOffset int
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		if name == new.GetName() {
			val = new.GetValue()
			newOffset = len(params) + 1
		} else {
			val = tbmeta.GetDefaultValue(name)
		}

		params = append(params, val)

		b.AppendString(fmt.Sprintf("$%d", len(params)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	real_field_name := tbmeta.GetRealFieldName(new.GetName())
	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")
	b.AppendString(fmt.Sprintf(" %s = $%d,", real_field_name, newOffset))
	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1", real_tab_name))
	b.AppendString(fmt.Sprintf(" where %s.__key__ = $1", real_tab_name))
	params = append(params, old.GetValue())
	b.AppendString(fmt.Sprintf(" and ( %s.%s = $%d or %s.__version__ < 0);", real_tab_name, real_field_name, len(params), real_tab_name))

	return b, params

}

func prepareCompareAndSetNxMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, old *proto.Field, new *proto.Field) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")

	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {
		var val interface{}
		if name == new.GetName() {
			val = new.GetValue()
		} else {
			val = tbmeta.GetDefaultValue(name)
		}
		params = append(params, val)

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	real_field_name := tbmeta.GetRealFieldName(new.GetName())

	params = append(params, old.GetValue())
	params = append(params, new.GetValue())

	b.AppendString(fmt.Sprintf("%s=if(__version__ < 0 or %s = ? ,?,%s),", real_field_name, real_field_name, real_field_name))

	params = append(params, new.GetValue())
	//real_field_name=new表示前面的if执行成功
	b.AppendString(fmt.Sprintf(" __version__ = if(%s = ?, abs(__version__)+1,__version__);", real_field_name))

	return b, params
}

func CompareAndSetNx(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, old *proto.Field, new *proto.Field) (int64, *proto.Field, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if nil != err {
		return 0, nil, err
	}

	params := []interface{}{key, 1, slot}
	b := buffer.New()
	if dbtype == "mysql" {
		b, params = prepareCompareAndSetNxMySql(params, b, tbmeta, old, new)
	} else {
		b, params = prepareCompareAndSetNxPgSql(params, b, tbmeta, old, new)
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
	var retField *proto.Field

	real_field_name := tbmeta.GetRealFieldName(old.GetName())

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

	fmt.Println("rowsAffected", rowsAffected)

	if rowsAffected > 0 {
		return v, retField, nil
	} else {
		return v, retField, ErrCompareNotEqual
	}
}

func prepareAddPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, addField *proto.Field) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	var addOffset int
	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {
		var val interface{}
		if name == addField.GetName() {
			val = addField.GetValue()
			addOffset = len(params) + 1
		} else {
			val = tbmeta.GetDefaultValue(name)
		}

		params = append(params, val)

		b.AppendString(fmt.Sprintf("$%d", len(params)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	real_field_name := tbmeta.GetRealFieldName(addField.GetName())
	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")
	b.AppendString(fmt.Sprintf(" %s = %s.%s+$%d,", real_field_name, real_tab_name, real_field_name, addOffset))
	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1", real_tab_name))
	b.AppendString(fmt.Sprintf(" where %s.__key__ = $1;", real_tab_name))

	return b, params
}

func prepareAddMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, addField *proto.Field) (*buffer.Buffer, []interface{}) {
	b.AppendString(tbmeta.GetInsertPrefix()).AppendString("?,?,?,")

	fields := tbmeta.GetAllFieldsName()
	for i, name := range fields {
		var val interface{}
		if name == addField.GetName() {
			val = addField.GetValue()
		} else {
			val = tbmeta.GetDefaultValue(name)
		}
		params = append(params, val)

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")

	real_field_name := tbmeta.GetRealFieldName(addField.GetName())

	params = append(params, addField.GetValue())

	b.AppendString(fmt.Sprintf(" %s=%s+? , ", real_field_name, real_field_name))

	b.AppendString("__version__ = abs(__version__)+1;")

	return b, params
}

func Add(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, addField *proto.Field) (int64, *proto.Field, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if nil != err {
		return 0, nil, err
	}

	params := []interface{}{key, 1, slot}

	b := buffer.New()
	if dbtype == "mysql" {
		b, params = prepareAddMySql(params, b, tbmeta, addField)
	} else {
		b, params = prepareAddPgSql(params, b, tbmeta, addField)
	}

	//fmt.Println(b.ToStrUnsafe())

	_, err = tx.ExecContext(ctx, b.ToStrUnsafe(), params...)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	}

	//rowsAffected, _ := r.RowsAffected()
	var v int64
	var retField *proto.Field

	real_field_name := tbmeta.GetRealFieldName(addField.GetName())

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("select __version__,%s from %s where __key__ = '%s';", real_field_name, tbmeta.GetRealTableName(), key))

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	}

	defer rows.Close()

	field_receiver := addField.GetValueReceiver()
	field_convter := addField.GetValueConvtor()

	if rows.Next() {
		err := rows.Scan(&v, field_receiver)
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return 0, nil, err
			}
			return 0, nil, err
		} else {
			retField = proto.PackField(addField.GetName(), field_convter(field_receiver))
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}

	return v, retField, nil
}
