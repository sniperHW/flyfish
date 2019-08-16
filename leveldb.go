package flyfish

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"strings"
	"time"
)

var (
	levelDB *leveldb.DB
)

func leveldbInit() bool {
	var err error
	levelDB, err = leveldb.OpenFile("path/to/db", nil)
	if nil != err {
		return false
	}
	return true
}

//return table, key, unikey, fileName
func fetchUnikeyAndFieldName(key []byte) (string, string, string, string, error) {
	s := string(key)
	t := strings.Split(s, ":")
	if !(len(t) >= 3) {
		return "", "", "", "", fmt.Errorf("invaild key")
	} else {
		return t[1], strings.Join(t[2:], ":"), strings.Join(t[1:], ":"), t[0], nil
	}
}

func fetchFieldFromLevelDBValue(meta *table_meta, fieldName string, value []byte) *proto.Field {
	//to do,对配置配型和存储的类型做检查
	tt := proto.ValueType(value[0])
	switch tt {
	case proto.ValueType_string:
		return proto.PackField(fieldName, string(value[1:]))
	case proto.ValueType_float:
		u := binary.BigEndian.Uint64(value[1:])
		f := math.Float64frombits(u)
		return proto.PackField(fieldName, f)
	case proto.ValueType_int:
		i := binary.BigEndian.Uint64(value[1:])
		return proto.PackField(fieldName, int64(i))
	case proto.ValueType_uint:
		u := binary.BigEndian.Uint64(value[1:])
		return proto.PackField(fieldName, u)
	case proto.ValueType_blob:
		return proto.PackField(fieldName, value[1:])
	default:
		panic("invaild value type")
	}
}

var loadKeyCount int

func processLoadLevelDBKV(table, key, unikey, fieldName string, value []byte) {

	unit := getUnitByUnikey(unikey)
	unit.mtx.Lock()
	k, ok := unit.cacheKeys[unikey]
	if !ok {
		Debugln("newCacheKey")
		k = newCacheKey(unit, table, key, unikey)
		if nil == k {
			Fatalln("processLoadLevelDBKV newCacheKey falied")
		}
		k.status = cache_new
		k.values = map[string]*proto.Field{}
		unit.cacheKeys[unikey] = k
		unit.updateLRU(k)
	}

	v := fetchFieldFromLevelDBValue(k.meta, fieldName, value)
	if fieldName != "__version__" {
		k.values[fieldName] = v
	} else {
		loadKeyCount++
		k.version = v.GetInt()
		k.status = cache_ok
	}

	Debugln("--------------------------", v)
	for n, v := range k.values {
		Debugln(n, v.GetName(), v.GetValue())
	}
	Debugln("--------------------------")

	unit.mtx.Unlock()
}

func loadFromLevelDB() error {
	beg := time.Now()
	iter := levelDB.NewIterator(nil, nil)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()

		table, key, unikey, fieldName, err := fetchUnikeyAndFieldName(k)

		if nil != err {
			return err
		}

		processLoadLevelDBKV(table, key, unikey, fieldName, v)

	}
	iter.Release()
	Infoln("loadFromLevelDB load time", time.Now().Sub(beg), loadKeyCount)
	return iter.Error()
}

func levelDBWrite(batch *leveldb.Batch, tt int, unikey string, meta *table_meta, fields map[string]*proto.Field, version int64) {

	Debugln("levelDBWrite")
	//leveldb key  fieldname:unikey

	if tt == write_back_delete {
		key := strGet()
		val := strGet()
		for n, _ := range meta.fieldMetas {
			key.append(n).append(":").append(unikey)
			batch.Delete(key.bytes())
			key.reset()
		}

		//将版本号设置为0
		key.append("__version__:").append(unikey)
		val.appendByte(byte(proto.ValueType_int)).appendInt64(version)
		batch.Put(key.bytes(), val.bytes())

		strPut(key)
		strPut(val)
	} else {
		key := strGet()
		val := strGet()
		for n, v := range fields {
			if n != "__version__" {
				key.append(v.GetName()).append(":").append(unikey)
				val.appendField(v)
				batch.Put(key.bytes(), val.bytes())
				key.reset()
				val.reset()
			}
		}

		key.append("__version__:").append(unikey)
		val.appendByte(byte(proto.ValueType_int)).appendInt64(version)
		batch.Put(key.bytes(), val.bytes())

		strPut(key)
		strPut(val)
	}
}
