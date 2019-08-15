package flyfish

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"strings"
	"unsafe"
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

func fetchUnikeyAndFieldName(key []byte) (string, string, string, string, error) {
	//key shoud be table:__key__:filedname
	s := *(*string)(unsafe.Pointer(&key))
	t := strings.Split(s, ":")
	if len(t) != 3 {
		return "", "", "", "", fmt.Errorf("invaild key")
	} else {
		return t[0], t[1], t[0] + ":" + t[1], t[2], nil
	}
}

func fetchFieldFromLevekDBValue(meta *table_meta, fileName string, value []byte) *proto.Field {
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

func processLoadLevelDBKV(table, key, unikey, fileName string, value []byte) {
	unit := getUnitByUnikey(unikey)
	unit.mtx.Lock()
	k, ok := unit.cacheKeys[unikey]
	if !ok {
		k = newCacheKey(unit, table, key, unikey)
		if nil == k {
			Fatalln("processLoadLevelDBKV newCacheKey falied")
		}
		k.values = map[string]*proto.Field{}
		unit.cacheKeys[unikey] = k
		unit.updateLRU(k)
	}
	k.values[fileName] = fetchFieldFromLevekDBValue(k.meta, fileName, value)
	unit.mtx.Unlock()
}

func loadFromLevelDB() error {
	Infoln("loadFromLevelDB")
	iter := levelDB.NewIterator(nil, nil)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()

		Infoln(string(k))

		table, key, unikey, fileName, err := fetchUnikeyAndFieldName(k)

		if nil != err {
			return err
		}

		processLoadLevelDBKV(table, key, unikey, fileName, v)

	}
	iter.Release()
	return iter.Error()
}

func levelDBWrite(tt int, unikey string, meta *table_meta, fields map[string]*proto.Field) error {

	Debugln("levelDBWrite")

	batch := new(leveldb.Batch)

	if tt == write_back_delete {
		key := strGet()
		val := strGet()
		for n, _ := range meta.fieldMetas {
			key.append(unikey).append(":").append(n)
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

		Debugln("levelDBWrite put")

		key := strGet()
		val := strGet()
		for _, v := range fields {
			key.append(unikey).append(":").append(v.GetName())
			val.appendField(v)
			batch.Put(key.bytes(), val.bytes())
			key.reset()
			val.reset()
		}
		strPut(key)
		strPut(val)
	}
	return levelDB.Write(batch, nil)
}
