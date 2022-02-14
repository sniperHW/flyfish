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
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.LastWriteBackVersion)
	b.AppendString(fmt.Sprintf("delete from %s where __key__ = $1 and __version__ = $2;", meta.real_tableName))
	this.b = b
}

func (this *sqlExec) prepareMarkDelete(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.LastWriteBackVersion)
	b.AppendString(fmt.Sprintf("update %s set __version__ = 0 where __key__ = $1 and __version__ = $2;", meta.real_tableName))
	this.b = b
}

func (this *sqlExec) prepareUpdate(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.LastWriteBackVersion)

	b.AppendString(fmt.Sprintf("update %s set ", meta.real_tableName))

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			this.args = append(this.args, v.GetValue())
			b.AppendString(fmt.Sprintf("%s=$%d,", meta.getRealFieldName(k), len(this.args)))
		}
	}

	b.AppendString("__version__=$2 where __key__ = $1 and __version__ = $3;")
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
	this.args = append(this.args, s.LastWriteBackVersion)

	this.__prepareInsert(b, s)
	if this.sqlType == "pgsql" {
		b.AppendString(" ON conflict(__key__)  DO UPDATE SET ")
		for i, name := range meta.queryMeta.real_field_names[4:] {
			b.AppendString(fmt.Sprintf("%s=$%d,", name, i+5))
		}
		b.AppendString(fmt.Sprintf("__version__=$2 where %s.__key__ = $1 and %s.__version__ = $4;", meta.real_tableName, meta.real_tableName))
	} else {
		b.AppendString(" on duplicate key update ")
		for i, name := range meta.queryMeta.real_field_names[4:] {
			b.AppendString(fmt.Sprintf("%s=if(__version__ = $4,$%d,%s),", name, i+5, name))
		}

		b.AppendString("__version__=if(__version__ = $4,$2,__version__);")
	}
	this.b = b
}

func (this *sqlExec) exec(dbc *sqlx.DB) (rowsAffected int64, err error) {
	r, err := dbc.Exec(this.b.ToStrUnsafe(), this.args...)
	if nil == err {
		rowsAffected, _ = r.RowsAffected()
	}
	return
}
