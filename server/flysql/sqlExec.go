package flysql

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/proto"
	"strings"
)

var ErrRecordNotExist error = errors.New("record not exist")
var ErrCompareNotEqual error = errors.New("compare not equal")
var ErrRecordNotChange error = errors.New("record not change")

func txUpdate(ctx context.Context, tx *dbsql.Tx, str string, params []interface{}) (int64, error) {
	r, err := tx.ExecContext(ctx, str, params...)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, err
		}
		return 0, err
	}

	rowsAffected, _ := r.RowsAffected()
	return rowsAffected, err
}

func txSelect(ctx context.Context, tx *dbsql.Tx, tbmeta *sql.TableMeta, key string, fields []*proto.Field) (int64, []*proto.Field, error) {
	wantFields := []string{"__version__"}
	for _, v := range fields {
		wantFields = append(wantFields, tbmeta.GetRealFieldName(v.GetName()))
	}

	var version int64
	receiver := []interface{}{&version}
	for _, v := range fields {
		receiver = append(receiver, v.GetValueReceiver())
	}

	err := tx.QueryRowContext(ctx, fmt.Sprintf("select %s from %s where __key__ = '%s';", strings.Join(wantFields, ","), tbmeta.GetRealTableName(), key)).Scan(receiver...)
	switch {
	case err == dbsql.ErrNoRows:
		return 0, nil, nil
	case err != nil:
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return 0, nil, err
		}
		return 0, nil, err
	default:
		for i, v := range fields {
			fields[i] = proto.PackField(v.GetName(), v.GetValueConvertor()(receiver[i+1]))
		}

		return version, fields, nil
	}
}

func prepareInsertPgsql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}, [][]interface{}) {
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

	return b, params, ff
}

func prepareInsertMysql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}, [][]interface{}) {
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

	return b, params, ff
}

func prepareMarkDeletePgsql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta) (*buffer.Buffer, []interface{}) {
	b, params, _ = prepareInsertPgsql(params, b, tbmeta, map[string]*proto.Field{})

	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(fmt.Sprintf("__version__ = 0-(abs(%s.__version__)+1) where %s.__key__ = $1 and %s.__version__ > 0;", real_tab_name, real_tab_name, real_tab_name))
	return b, params
}

func prepareMarkDeleteMysql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta) (*buffer.Buffer, []interface{}) {
	b, params, ff := prepareInsertMysql(params, b, tbmeta, map[string]*proto.Field{})

	for _, v := range ff {
		b.AppendString(fmt.Sprintf("%s=if(__version__ > 0,?,%s),", v[0].(string), v[0].(string)))
		params = append(params, v[1])
	}
	b.AppendString(" __version__ = if(__version__ > 0, 0-(abs(__version__)+1),__version__);")

	return b, params
}

func MarkDelete(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int) (int64, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if _, err = txUpdate(ctx, tx, b.ToStrUnsafe(), params); err != nil {
		return 0, err
	}

	if version, _, err := txSelect(ctx, tx, tbmeta, key, nil); err != nil {
		return 0, err
	} else if err = tx.Commit(); err != nil {
		return 0, err
	} else {
		return version, nil
	}
}

func prepareSetPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field, version ...int64) (*buffer.Buffer, []interface{}) {
	b, params, ff := prepareInsertPgsql(params, b, tbmeta, setFields)

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
	b, params, ff := prepareInsertMysql(params, b, tbmeta, setFields)

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

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if _, err = txUpdate(ctx, tx, b.ToStrUnsafe(), params); nil != err {
		return 0, err
	}

	if version, _, err := txSelect(ctx, tx, tbmeta, key, nil); err != nil {
		return 0, err
	} else if err = tx.Commit(); err != nil {
		return 0, err
	} else {
		return version, nil
	}
}

func prepareSetNxPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}) {
	b, params, ff := prepareInsertPgsql(params, b, tbmeta, setFields)

	for _, v := range ff {
		b.AppendString(fmt.Sprintf(" %s = $%d,", v[0].(string), v[1].(int)))
	}

	real_tab_name := tbmeta.GetRealTableName()

	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1 where %s.__key__ = $1 and %s.__version__ < 0;", real_tab_name, real_tab_name, real_tab_name))

	return b, params

}

func prepareSetNxMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, setFields map[string]*proto.Field) (*buffer.Buffer, []interface{}) {
	b, params, ff := prepareInsertMysql(params, b, tbmeta, setFields)
	for _, v := range ff {
		b.AppendString(fmt.Sprintf("%s=if(__version__ < 0,?,%s),", v[0].(string), v[0].(string)))
		params = append(params, v[1])
	}

	b.AppendString(" __version__ = if(__version__ < 0, abs(__version__)+1,__version__);")
	return b, params
}

func SetNx(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, fields map[string]*proto.Field) (int64, map[string]*proto.Field, error) {

	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if rowsAffected, err := txUpdate(ctx, tx, b.ToStrUnsafe(), params); nil != err {
		return 0, nil, err
	} else {

		var wantFields []*proto.Field

		if !(rowsAffected > 0) {
			for _, v := range fields {
				wantFields = append(wantFields, v)
			}
		}

		if version, wantFields, err := txSelect(ctx, tx, tbmeta, key, wantFields); err != nil {
			return 0, nil, err
		} else if err = tx.Commit(); err != nil {
			return 0, nil, err
		} else {
			var retFields map[string]*proto.Field
			if len(wantFields) > 0 {
				retFields = map[string]*proto.Field{}
				for _, v := range wantFields {
					retFields[v.GetName()] = v
				}
			}
			return version, retFields, nil
		}
	}
}

func CompareAndSet(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, old *proto.Field, new *proto.Field) (int64, *proto.Field, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if rowsAffected, err := txUpdate(ctx, tx, updateStr, params); nil != err {
		return 0, nil, err
	} else {

		if version, retFields, err := txSelect(ctx, tx, tbmeta, key, []*proto.Field{new}); err != nil {
			return 0, nil, err
		} else if err = tx.Commit(); err != nil {
			return 0, nil, err
		} else if rowsAffected > 0 {
			return version, retFields[0], nil
		} else if version != 0 {
			return version, retFields[0], ErrCompareNotEqual
		} else {
			return version, nil, ErrRecordNotExist
		}
	}
}

func prepareCompareAndSetNxPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, old *proto.Field, new *proto.Field) (*buffer.Buffer, []interface{}) {
	fields := map[string]*proto.Field{}
	fields[new.GetName()] = new
	b, params, ff := prepareInsertPgsql(params, b, tbmeta, fields)

	for _, v := range ff {
		b.AppendString(fmt.Sprintf(" %s = $%d,", v[0].(string), v[1].(int)))
	}

	real_tab_name := tbmeta.GetRealTableName()
	real_field_name := tbmeta.GetRealFieldName(new.GetName())
	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1", real_tab_name))
	b.AppendString(fmt.Sprintf(" where %s.__key__ = $1", real_tab_name))
	params = append(params, old.GetValue())
	b.AppendString(fmt.Sprintf(" and ( %s.%s = $%d or %s.__version__ < 0);", real_tab_name, real_field_name, len(params), real_tab_name))
	return b, params
}

func prepareCompareAndSetNxMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, old *proto.Field, new *proto.Field) (*buffer.Buffer, []interface{}) {
	fields := map[string]*proto.Field{}
	fields[new.GetName()] = new

	b, params, _ = prepareInsertMysql(params, b, tbmeta, fields)

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
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if rowsAffected, err := txUpdate(ctx, tx, b.ToStrUnsafe(), params); nil != err {
		return 0, nil, err
	} else {
		if version, retFields, err := txSelect(ctx, tx, tbmeta, key, []*proto.Field{new}); err != nil {
			return 0, nil, err
		} else if err = tx.Commit(); err != nil {
			return 0, nil, err
		} else if rowsAffected > 0 {
			return version, retFields[0], nil
		} else {
			return version, retFields[0], ErrCompareNotEqual
		}
	}
}

func prepareAddPgSql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, addField *proto.Field) (*buffer.Buffer, []interface{}) {
	fields := map[string]*proto.Field{}
	fields[addField.GetName()] = addField
	b, params, _ = prepareInsertPgsql(params, b, tbmeta, fields)
	real_field_name := tbmeta.GetRealFieldName(addField.GetName())
	real_tab_name := tbmeta.GetRealTableName()
	params = append(params, addField.GetValue())
	b.AppendString(fmt.Sprintf(" %s = %s.%s+$%d,", real_field_name, real_tab_name, real_field_name, len(params)))
	b.AppendString(fmt.Sprintf(" __version__ = abs(%s.__version__)+1", real_tab_name))
	b.AppendString(fmt.Sprintf(" where %s.__key__ = $1;", real_tab_name))
	return b, params
}

func prepareAddMySql(params []interface{}, b *buffer.Buffer, tbmeta *sql.TableMeta, addField *proto.Field) (*buffer.Buffer, []interface{}) {
	fields := map[string]*proto.Field{}
	fields[addField.GetName()] = addField
	b, params, _ = prepareInsertMysql(params, b, tbmeta, fields)

	real_field_name := tbmeta.GetRealFieldName(addField.GetName())

	params = append(params, addField.GetValue())

	b.AppendString(fmt.Sprintf(" %s=%s+? , ", real_field_name, real_field_name))

	b.AppendString("__version__ = abs(__version__)+1;")

	return b, params
}

func Add(ctx context.Context, dbc *sqlx.DB, dbtype string, tbmeta *sql.TableMeta, key string, slot int, addField *proto.Field) (int64, *proto.Field, error) {
	tx, err := dbc.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelRepeatableRead})
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

	if _, err = txUpdate(ctx, tx, b.ToStrUnsafe(), params); nil != err {
		return 0, nil, err
	} else {
		if version, retFields, err := txSelect(ctx, tx, tbmeta, key, []*proto.Field{addField}); err != nil {
			return 0, nil, err
		} else if err = tx.Commit(); err != nil {
			return 0, nil, err
		} else {
			return version, retFields[0], nil
		}
	}
}

func Load(ctx context.Context, dbc *sqlx.DB, tbmeta *sql.TableMeta, key string, wantFields []string, version ...int64) (int64, []*proto.Field, error) {
	var retVersion int64
	var retFields []*proto.Field
	var err error

	fieldRealNames := []string{"__version__"}
	receivers := []interface{}{proto.ValueReceiverFactory(proto.ValueType_int)}
	convetors := []func(interface{}) interface{}{proto.GetValueConvertor(proto.ValueType_int)}

	if len(wantFields) == 0 {
		for _, f := range tbmeta.GetFieldMetas() {
			fieldRealNames = append(fieldRealNames, f.GetRealName())
			receivers = append(receivers, proto.ValueReceiverFactory(f.Type())())
			convetors = append(convetors, proto.GetValueConvertor(f.Type()))
		}
	} else {
		fieldMetas := tbmeta.GetFieldMetas()
		for _, v := range wantFields {
			f, ok := fieldMetas[v]
			if !ok {
				return 0, nil, fmt.Errorf("fileds %s not define in table:%s", v, tbmeta.TableName())
			}
			fieldRealNames = append(fieldRealNames, f.GetRealName())
			receivers = append(receivers, proto.ValueReceiverFactory(f.Type())())
			convetors = append(convetors, proto.GetValueConvertor(f.Type()))
		}
	}

	queryStr := fmt.Sprintf("select %s from %s where __key__ = '%s'", strings.Join(fieldRealNames, ","), tbmeta.GetRealTableName(), key)

	if len(version) > 0 {
		err = dbc.QueryRowContext(ctx, fmt.Sprintf("select __version__ from %s where __key__ = '%s';\n", tbmeta.GetRealTableName(), key)).Scan(&retVersion)
		switch {
		case err == dbsql.ErrNoRows:
			return 0, nil, ErrRecordNotExist
		case err != nil:
			return 0, nil, err
		case retVersion < 0:
			return retVersion, nil, ErrRecordNotExist
		default:
			queryStr += fmt.Sprintf(" and __version__ != %d", version[0])
		}
	}

	err = dbc.QueryRowContext(ctx, queryStr+";").Scan(receivers...)
	switch {
	case err == dbsql.ErrNoRows:
		if len(version) > 0 {
			return version[0], nil, ErrRecordNotChange
		} else {
			return 0, nil, ErrRecordNotExist
		}
	case err != nil:
		return 0, nil, err
	default:
		retVersion = convetors[0](receivers[0]).(int64)
		if retVersion < 0 {
			return retVersion, nil, ErrRecordNotExist
		} else {
			for i := 1; i < len(fieldRealNames); i++ {
				retFields = append(retFields, proto.PackField(wantFields[i-1], convetors[i](receivers[i])))
			}
			return retVersion, retFields, nil
		}
	}
}
