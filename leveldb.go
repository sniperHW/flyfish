package flyfish

import (
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	levelDB *DB
)

func leveldbInit() bool {
	levelDB, err := leveldb.OpenFile("path/to/db", nil)
	if nil != err {
		return false
	}
	return true
}

func levelDBBatch(batch *leveldb.Batch, tt int, unikey string, meta *table_meta, fields ...*proto.Field) {
	if tt == write_back_delete {
		s := strGet()
		for n, _ := range meta.fieldMetas {
			s.append(unikey).append(":").append(n)
			batch.Delete(s.bytes())
			s.reset()
		}

		s.append(unikey).append(":").append("__version__")
		batch.Delete(s.bytes())
		s.reset()

		s.append(unikey).append(":").append("writeBackVersion")
		batch.Delete(s.bytes())
		s.reset()

		s.append(unikey).append(":").append("writeBacked")
		batch.Delete(s.bytes())
		s.reset()

		strPut(s)

	} else {
		key := strGet()
		val := strGet()
		for _, v := range fields {
			key.append(unikey).append(":").append(v.GetName())
			val.append(v)
			key.reset()
			val.reset()
		}
		strPut(key)
		strPut(val)
	}
}
