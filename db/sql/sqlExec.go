package sql

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

type sqlExec struct {
	args    []interface{}
	sqlType string
	b       *buffer.Buffer
}

func (this *sqlExec) prepareDelete(b *buffer.Buffer, s *db.UpdateState) {
	this.args = this.args[:0]
	meta := s.Meta.(*TableMeta)
	b.AppendString("delete from ").AppendString(meta.real_tableName).AppendString(" where __key__ = $1;")
	this.args = append(this.args, s.Key)
	this.b = b
}

func (this *sqlExec) prepareUpdate(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)

	b.AppendString("update ").AppendString(meta.real_tableName).AppendString(" set ")

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			this.args = append(this.args, v.GetValue())
			b.AppendString(meta.getRealFieldName(k)).AppendString(fmt.Sprintf("=$%d,", len(this.args)))
		}
	}

	b.AppendString("__version__=$2 where __key__ = $1;")
	this.b = b
}

func (this *sqlExec) __prepareInsert(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)

	b.AppendString(meta.GetInsertPrefix()).AppendString("$1,$2,$3,")
	fields := meta.GetAllFieldsName()
	for i, name := range fields {

		var val interface{}

		v, ok := s.Fields[name]
		if !ok {
			val = meta.GetDefaultValue(name)
		} else {
			val = v.GetValue()
		}

		this.args = append(this.args, val)

		b.AppendString(fmt.Sprintf("$%d", len(this.args)))

		if i != len(fields)-1 {
			b.AppendString(",")
		}
		i++
	}
	b.AppendString(")")
}

/*
 *pgsql INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;
 *mysql insert into %s(%s) values(%s) on duplicate key update %s;
 */

func (this *sqlExec) prepareInsertUpdate(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.Slot)

	this.__prepareInsert(b, s)
	if this.sqlType == "pgsql" {
		b.AppendString(" ON conflict(__key__)  DO UPDATE SET ")
	} else {
		b.AppendString(" on duplicate key update ")
		b.AppendString(" ON conflict(__key__)  DO UPDATE SET ")
	}

	for i, name := range meta.queryMeta.real_field_names[3:] {
		b.AppendString(fmt.Sprintf("%s=$%d,", name, i+4))
	}

	if this.sqlType == "pgsql" {
		b.AppendString("__version__=$2 ")
		b.AppendString(" where ").AppendString(s.Meta.(*TableMeta).real_tableName).AppendString(".__key__ = $1;")
	} else {
		b.AppendString("__version__=$2;")
	}

	this.b = b
}

func (this *sqlExec) exec(dbc *sqlx.DB) (err error) {
	_, err = dbc.Exec(this.b.ToStrUnsafe(), this.args...)
	return
}

/*
func (this *sqlstring) updateStatement(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString("update ").AppendString(meta.real_tableName).AppendString(" set ")
	version := proto.PackField("__version__", s.Version)

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(meta.getRealFieldName(k)).AppendString("=")
			this.appendFieldStr(b, v)
			b.AppendString(",")
		}
	}

	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(" where __key__ = '").AppendString(s.Key).AppendString("';")

	//GetSugar().Debug(b.ToStrUnsafe())
}
*/

/*func binaryTopgSqlStr(b *buffer.Buffer, bytes []byte) {
	b.AppendString("'")
	for _, v := range bytes {
		b.AppendString(pgsqlByteToString[int(v)])
	}
	b.AppendString("'::bytea")
}

func binaryTomySqlStr(b *buffer.Buffer, bytes []byte) {
	b.AppendString("unhex('")
	for _, v := range bytes {
		b.AppendString(mysqlByteToString[int(v)])
	}
	b.AppendString("')")
}

type sqlstring struct {
	binarytostr             func(b *buffer.Buffer, bytes []byte)
	buildInsertUpdateString func(b *buffer.Buffer, s *db.UpdateState, version *proto.Field)
}

func (this *sqlstring) appendFieldStr(b *buffer.Buffer, field *proto.Field) {
	tt := field.GetType()

	switch tt {
	case proto.ValueType_string:
		b.AppendString("'").AppendString(field.GetString()).AppendString("'")
	case proto.ValueType_float:
		b.AppendString(fmt.Sprintf("%f", field.GetFloat()))
	case proto.ValueType_int:
		b.AppendString(strconv.FormatInt(field.GetInt(), 10))
	case proto.ValueType_blob:
		this.binarytostr(b, field.GetBlob())
	}
}

func (this *sqlstring) buildInsert(b *buffer.Buffer, s *db.UpdateState) *proto.Field {
	meta := s.Meta.(*TableMeta)

	version := proto.PackField("__version__", s.Version)
	slot := proto.PackField("__slot__", s.Slot)

	b.AppendString(meta.GetInsertPrefix()).AppendString("'").AppendString(s.Key).AppendString("',")

	this.appendFieldStr(b, version)
	b.AppendString(",")

	this.appendFieldStr(b, slot)
	b.AppendString(",")

	fields := meta.GetAllFieldsName()
	i := 0
	for _, name := range fields {
		v, ok := s.Fields[name]
		if !ok {
			v = proto.PackField(name, meta.GetDefaultValue(name))
		}
		this.appendFieldStr(b, v)
		if i != len(fields)-1 {
			b.AppendString(",")
		}
		i++
	}

	b.AppendString(")")
	return version
}



func (this *sqlstring) insertUpdateStatementPgSql(b *buffer.Buffer, s *db.UpdateState, version *proto.Field) {
	meta := s.Meta.(*TableMeta)
	b.AppendString(" ON conflict(__key__)  DO UPDATE SET ")

	for _, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(meta.getRealFieldName(v.GetName())).AppendString("=")
			this.appendFieldStr(b, v)
			b.AppendString(",")
		}
	}

	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(" where ").AppendString(s.Meta.(*TableMeta).real_tableName).AppendString(".__key__ = '").AppendString(s.Key).AppendString("';")

	//GetSugar().Debug(b.ToStrUnsafe())
}

/*
 *insert into %s(%s) values(%s) on duplicate key update %s;
 * /

func (this *sqlstring) insertUpdateStatementMySql(b *buffer.Buffer, s *db.UpdateState, version *proto.Field) {
	meta := s.Meta.(*TableMeta)

	b.AppendString(" on duplicate key update ")

	for _, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(meta.getRealFieldName(v.GetName())).AppendString("=")
			this.appendFieldStr(b, v)
			b.AppendString(",")
		}
	}
	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(";")

	//GetSugar().Debug(b.ToStrUnsafe())
}

func (this *sqlstring) insertUpdateStatement(b *buffer.Buffer, s *db.UpdateState) {
	version := this.buildInsert(b, s)
	this.buildInsertUpdateString(b, s, version)
}

func (this *sqlstring) insertStatement(b *buffer.Buffer, s *db.UpdateState) {
	this.buildInsert(b, s)
	b.AppendString(";")
}

func (this *sqlstring) updateStatement(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString("update ").AppendString(meta.real_tableName).AppendString(" set ")
	version := proto.PackField("__version__", s.Version)

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(meta.getRealFieldName(k)).AppendString("=")
			this.appendFieldStr(b, v)
			b.AppendString(",")
		}
	}

	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(" where __key__ = '").AppendString(s.Key).AppendString("';")

	//GetSugar().Debug(b.ToStrUnsafe())
}

func (this *sqlstring) deleteStatement(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString("delete from ").AppendString(meta.real_tableName).AppendString(" where __key__ = '").AppendString(s.Key).AppendString("';")

	//GetSugar().Debug(b.ToStrUnsafe())
}*/
