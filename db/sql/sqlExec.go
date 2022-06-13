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

func (this *sqlExec) prepareMarkDeletePgsql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString(fmt.Sprintf("update %s set __version__ = $1 where __key__ = $2 and __version__ = $3;", meta.real_tableName))
	this.b = b
}

func (this *sqlExec) prepareMarkDeleteMysql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	b.AppendString(fmt.Sprintf("update %s set __version__ = ? where __key__ = ? and __version__ = ?;", meta.real_tableName))
	this.b = b
}

func (this *sqlExec) prepareMarkDelete(b *buffer.Buffer, s *db.UpdateState) {
	this.args = this.args[:0]
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.LastWriteBackVersion)
	if this.sqlType == "mysql" {
		this.prepareMarkDeleteMysql(b, s)
	} else {
		this.prepareMarkDeletePgsql(b, s)
	}
}

func (this *sqlExec) prepareUpdatePgsql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.LastWriteBackVersion)

	b.AppendString(fmt.Sprintf("update %s set ", meta.real_tableName))

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			this.args = append(this.args, v.GetValue())
			b.AppendString(fmt.Sprintf("%s=$%d,", meta.GetRealFieldName(k), len(this.args)))
		}
	}

	b.AppendString("__version__=$2 where __key__ = $1 and __version__ = $3;")
	this.b = b
}

func (this *sqlExec) prepareUpdateMysql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]

	b.AppendString(fmt.Sprintf("update %s set ", meta.real_tableName))

	for k, v := range s.Fields {
		if nil == meta.CheckFields(v) {
			b.AppendString(fmt.Sprintf("%s=?,", meta.GetRealFieldName(k)))
			this.args = append(this.args, v.GetValue())
		}
	}

	b.AppendString("__version__=? where __key__ = ? and __version__ = ?;")
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.LastWriteBackVersion)

	this.b = b

}

func (this *sqlExec) prepareUpdate(b *buffer.Buffer, s *db.UpdateState) {
	if this.sqlType == "mysql" {
		this.prepareUpdateMysql(b, s)
	} else {
		this.prepareUpdatePgsql(b, s)
	}
}

func (this *sqlExec) prepareInsertUpdatePgSql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)

	this.args = this.args[:0]
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.Slot)
	this.args = append(this.args, s.LastWriteBackVersion)

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

	b.AppendString(") ON conflict(__key__)  DO UPDATE SET ")
	for i, name := range meta.queryMeta.real_field_names[3:] {
		b.AppendString(fmt.Sprintf("%s=$%d,", name, i+5))
	}
	b.AppendString(fmt.Sprintf("__version__=$2 where %s.__key__ = $1 and %s.__version__ = $4;", meta.real_tableName, meta.real_tableName))

	this.b = b
}

func (this *sqlExec) prepareInsertUpdateMySql(b *buffer.Buffer, s *db.UpdateState) {
	meta := s.Meta.(*TableMeta)
	this.args = this.args[:0]

	b.AppendString(meta.GetInsertPrefix()).AppendString("?,?,?,")
	this.args = append(this.args, s.Key)
	this.args = append(this.args, s.Version)
	this.args = append(this.args, s.Slot)

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

		b.AppendString("?")

		if i != len(fields)-1 {
			b.AppendString(",")
		}
	}

	b.AppendString(") on duplicate key update ")
	for i, name := range meta.queryMeta.real_field_names[3:] {
		b.AppendString(fmt.Sprintf("%s=if(__version__ = ?,?,%s),", name, name))
		this.args = append(this.args, s.LastWriteBackVersion)
		this.args = append(this.args, this.args[i+3])
	}

	b.AppendString("__version__=if(__version__ = ?,?,__version__);")
	this.args = append(this.args, s.LastWriteBackVersion)
	this.args = append(this.args, s.Version)

	this.b = b
}

func (this *sqlExec) prepareInsertUpdate(b *buffer.Buffer, s *db.UpdateState) {
	if this.sqlType == "mysql" {
		this.prepareInsertUpdateMySql(b, s)
	} else {
		this.prepareInsertUpdatePgSql(b, s)
	}
}

func (this *sqlExec) exec(dbc *sqlx.DB) (rowsAffected int64, err error) {
	r, err := dbc.Exec(this.b.ToStrUnsafe(), this.args...)
	if nil == err {
		rowsAffected, _ = r.RowsAffected()
	}
	return
}
