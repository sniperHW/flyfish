package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
	"time"
)

func main() {

	levelDB, err := leveldb.OpenFile("path/to/db2", nil)
	if nil != err {
		fmt.Println(err)
		return
	}

	count := 0
	beg := time.Now()
	iter := levelDB.NewIterator(nil, nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	fmt.Println("loadFromLevelDB load time", time.Now().Sub(beg), count)

	val := []byte(strings.Repeat("a", 1024))

	beg = time.Now()

	batch := new(leveldb.Batch)

	for i := 1; i <= 1000000; i++ {
		key := fmt.Sprintf("key:%d", i)
		//levelDB.Put([]byte(key), []byte(val), nil)
		batch.Put([]byte(key), val)
		if i%1000 == 0 {
			levelDB.Write(batch, nil)
			batch.Reset()
		}
	}
	levelDB.Write(batch, nil)

	fmt.Println("loadFromLevelDB put time", time.Now().Sub(beg))
}
