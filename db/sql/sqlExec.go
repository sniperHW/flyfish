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
