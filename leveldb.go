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

func fetchFieldFromLevelDBValue(meta *table_meta, fileName string, value []byte) *proto.Field {
	//to do,对配置配型和存储的类型做检查
	tt := proto.ValueType(value[0])
	switch tt {
	case proto.ValueType_string:
		return proto.PackField(fileName, string(value[1:]))
	case proto.ValueType_float:
		u := binary.BigEndian.Uint64(value[1:])
		f := math.Float64frombits(u)
		return proto.PackField(fileName, f)
	case proto.ValueType_int:
		i := binary.BigEndian.Uint64(value[1:])
		return proto.PackField(fileName, int64(i))
	case proto.ValueType_uint:
		u := binary.BigEndian.Uint64(value[1:])
		return proto.PackField(fileName, u)
	case proto.ValueType_blob:
		return proto.PackField(fileName, value[1:])
	default:
		panic("invaild value type")
	}
}

var loadKeyCount int

func processLoadLevelDBKV(table, key, unikey, fileName string, value []byte) {

	//Debugln("fileName", fileName)

	unit := getUnitByUnikey(unikey)
	unit.mtx.Lock()
	k, ok := unit.cacheKeys[unikey]
	if !ok {
		Debugln("newCacheKey")
		k = newCacheKey(unit, table, key, unikey)
		if nil == k {
			Fatalln("processLoadLevelDBKV newCacheKey falied")
		}
		k.values = map[string]*proto.Field{}
		unit.cacheKeys[unikey] = k
		unit.updateLRU(k)
	}

	//Debugln(&k, &unit, unit.cacheKeys)

	v := fetchFieldFromLevelDBValue(k.meta, fileName, value)
	if fileName != "__version__" {
		k.values[fileName] = v

		Debugln("Set", v)

	} else {
		k.version = v.GetInt()
		k.status = cache_ok
		loadKeyCount++
		Infoln(unikey, "ok")
	}

	//Debugln(len(k.values), k.values)

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

		table, key, unikey, fileName, err := fetchUnikeyAndFieldName(k)

		//Infoln(table, key, unikey, fileName)

		if nil != err {
			return err
		}

		processLoadLevelDBKV(table, key, unikey, fileName, v)

	}
	iter.Release()
	Infoln("loadFromLevelDB load time", time.Now().Sub(beg), loadKeyCount)
	return iter.Error()
}

func levelDBWrite(batch *leveldb.Batch, tt int, unikey string, meta *table_meta, fields map[string]*proto.Field) {

	Debugln("levelDBWrite")

	//batch := new(leveldb.Batch)

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
		key.append("__version__")
		val.appendInt64(0)
		batch.Put(key.bytes(), val.bytes())

		strPut(key)
		strPut(val)
	} else {

		if len(fields) > 0 {
			Infoln("levelDBWrite put", unikey)
		}

		key := strGet()
		val := strGet()
		for _, v := range fields {
			key.append(v.GetName()).append(":").append(unikey)
			val.appendField(v)
			batch.Put(key.bytes(), val.bytes())
			key.reset()
			val.reset()
		}
		strPut(key)
		strPut(val)
	}
	//return levelDB.Write(batch, nil)
}
