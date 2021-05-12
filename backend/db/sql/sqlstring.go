package sql

import (
	"fmt"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
)

func binaryTopgSqlStr(b *buffer.Buffer, bytes []byte) {
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

	b.AppendString(meta.GetInsertPrefix()).AppendString("'").AppendString(s.Key).AppendString("',")

	this.appendFieldStr(b, version)
	b.AppendString(",")

	fields := meta.GetInsertOrder()
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

/*
 *INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;
 */

func (this *sqlstring) insertUpdateStatementPgSql(b *buffer.Buffer, s *db.UpdateState, version *proto.Field) {

	b.AppendString(" ON conflict(__key__)  DO UPDATE SET ")

	for _, v := range s.Fields {
		b.AppendString(v.GetName()).AppendString("=")
		this.appendFieldStr(b, v)
		b.AppendString(",")
	}

	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(" where ").AppendString(s.Meta.(*TableMeta).GetTable()).AppendString(".__key__ = '").AppendString(s.Key).AppendString("';")

	GetSugar().Debug(b.ToStrUnsafe())
}

/*
 *insert into %s(%s) values(%s) on duplicate key update %s;
 */

func (this *sqlstring) insertUpdateStatementMySql(b *buffer.Buffer, s *db.UpdateState, version *proto.Field) {

	b.AppendString(" on duplicate key update ")

	for _, v := range s.Fields {
		b.AppendString(v.GetName()).AppendString("=")
		this.appendFieldStr(b, v)
		b.AppendString(",")
	}
	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(";")

	GetSugar().Debug(b.ToStrUnsafe())
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
	b.AppendString("update ").AppendString(meta.GetTable()).AppendString(" set ")
	version := proto.PackField("__version__", s.Version)

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(k).AppendString("=")
			this.appendFieldStr(b, v)
			b.AppendString(",")
		}
	}

	b.AppendString("__version__=")
	this.appendFieldStr(b, version)
	b.AppendString(" where __key__ = '").AppendString(s.Key).AppendString("';")

	GetSugar().Debug(b.ToStrUnsafe())
}

func (this *sqlstring) deleteStatement(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString("delete from ").AppendString(meta.GetTable()).AppendString(" where __key__ = '").AppendString(s.Key).AppendString("';")

	GetSugar().Debug(b.ToStrUnsafe())
}
