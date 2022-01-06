package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/proto"
)

type cacheKv struct {
	key     string
	version int64
	fields  []*proto.Field //字段
}

type scanner struct {
	wantFields []string
	kv         []*cacheKv
	offset     int
	scanner    *sql.Scanner
	tbmeta     db.TableMeta
}

func (sc *scanner) makeCachekvs(table string, kvs map[string]*kv) {
	for _, v := range kvs {
		if v.state == kv_ok && v.table == table {
			ckv := &cacheKv{
				key:     v.key,
				version: v.version,
			}

			for _, k := range sc.wantFields {
				if f, ok := v.fields[k]; ok {
					ckv.fields = append(ckv.fields, f)
				}
			}

			sc.kv = append(sc.kv, ckv)
		}
	}
}

func (sc *scanner) fillDefault(fields []*proto.Field) []*proto.Field {
	for _, name := range sc.wantFields {
		found := false

		for _, v := range fields {
			if v.Name == name {
				found = true
				break
			}
		}

		//field没有被设置过，使用默认值
		if !found {
			if vv := sc.tbmeta.GetDefaultValue(name); nil != vv {
				fields = append(fields, proto.PackField(name, vv))
			}
		}
	}

	return fields
}

func (sc *scanner) next(count int) (rows []*proto.Row, err error) {
	if 0 >= count {
		count = 50
	} else if count > 200 {
		count = 200
	}

	if r, err := sc.scanner.Next(count); nil != err {
		return nil, err
	} else {
		for _, v := range r {
			rows = append(rows, &proto.Row{
				Key:     v.Key,
				Version: v.Version,
				Fields:  sc.fillDefault(v.Fields),
			})
			count--
		}
	}

	for count > 0 && sc.offset < len(sc.kv) {
		rows = append(rows, &proto.Row{
			Key:     sc.kv[sc.offset].key,
			Version: sc.kv[sc.offset].version,
			Fields:  sc.fillDefault(sc.kv[sc.offset].fields),
		})
		count--
		sc.offset++
	}

	return
}
