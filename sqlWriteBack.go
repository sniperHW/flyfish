package flyfish

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/util"
	"hash/crc64"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type sqlUpdater struct {
	db       *sqlx.DB
	name     string
	lastTime time.Time
	queue    *util.BlockQueue
	wg       *sync.WaitGroup
	sqlStr   *str
	records  []*proto.Record
	buffer   []byte
	replay   bool
}

func newSqlUpdater(db *sqlx.DB, name string, wg *sync.WaitGroup, replay bool) *sqlUpdater {
	if nil != wg {
		wg.Add(1)
	}
	return &sqlUpdater{
		name:    name,
		records: []*proto.Record{},
		queue:   util.NewBlockQueueWithName(name),
		db:      db,
		wg:      wg,
		replay:  replay,
	}
}

func isRetryError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	} else {
		switch err.(type) {
		case *net.OpError:
			return true
			break
		case net.Error:
			return true
			break
		default:
			break
		}
	}
	return false
}

func (this *sqlUpdater) process(path string) {

	Debugln("sqlUpdater process")

	beg := time.Now()

	var err error

	stat, err := os.Stat(path)

	if nil != err {
		Fatalln("open file failed:", path, err)
		return
	}

	f, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)

	if nil != err {
		Fatalln("open file failed:", path, err)
		return
	}

	if nil == this.buffer || cap(this.buffer) < int(stat.Size()) {
		this.buffer = make([]byte, sizeofPow2(int(stat.Size())))
	}

	n, err := f.Read(this.buffer)

	f.Close()

	loadTime := time.Now().Sub(beg)

	if n != (int)(stat.Size()) {
		Fatalln("read file failed:", path, err)
		return
	}

	checkSum := binary.BigEndian.Uint64(this.buffer[n-8:])

	//校验数据
	if checkSum != crc64.Checksum(this.buffer[:n-8], crc64Table) {
		Fatalln("checkSum failed:", path)
		return
	}

	this.records = this.records[0:0]
	if nil == this.sqlStr {
		this.sqlStr = &str{
			data: make([]byte, strInitCap),
			cap:  strInitCap,
			len:  0,
		}
	} else {
		this.sqlStr.reset()
	}

	offset := 0
	end := n - 8
	for offset < end {
		pbRecord := &proto.Record{}
		l := int(binary.BigEndian.Uint32(this.buffer[offset : offset+4]))
		offset += 4
		if err = pb.Unmarshal(this.buffer[offset:offset+l], pbRecord); err != nil {
			Fatalln("replayRecord error ,offset:", offset, err)
		}
		offset += l

		meta := getMetaByTable(pbRecord.GetTable())

		if nil == meta {
			Fatalln("replayRecord error invaild table ,offset:", offset-l, pbRecord.GetTable())
		}

		this.records = append(this.records, pbRecord)

		tt := pbRecord.GetType()
		if tt == proto.SqlType_insert {
			if this.replay {
				buildInsertUpdateString(this.sqlStr, pbRecord, meta)
			} else {
				buildInsertString(this.sqlStr, pbRecord, meta)
			}
		} else if tt == proto.SqlType_update {
			buildUpdateString(this.sqlStr, pbRecord, meta)
		} else if tt == proto.SqlType_delete {
			buildDeleteString(this.sqlStr, pbRecord)
		} else {
			Fatalln("replayRecord invaild tt,offset:", offset)
		}
	}

	recordCount := len(this.records)

	for {
		_, err = this.db.Exec(this.sqlStr.toString())
		if nil == err {
			for _, v := range this.records {
				uniKey := fmt.Sprintf("%s:%s", v.GetTable(), v.GetKey())
				unit := getUnitByUnikey(uniKey)
				unit.mtx.Lock()
				k, ok := unit.cacheKeys[uniKey]
				unit.mtx.Unlock()
				if ok {
					k.clearWriteBack(v.GetWritebackVersion())
				}
			}
			break
		} else {
			Errorln(this.sqlStr.toString(), err)
			if isRetryError(err) {
				Errorln("sqlUpdater exec error:", err)
				if isStop() {
					return
				}
				//休眠一秒重试
				time.Sleep(time.Second)
			} else {
				Errorln("sqlUpdater exec error:", err, path)
			}
		}
	}

	sqlTime := time.Now().Sub(beg)

	//删除文件
	os.Remove(path)

	totalTime := time.Now().Sub(beg)

	Infoln("loadTime:", loadTime, "recordCount:", recordCount, "sqlTime:", sqlTime, "totalTime:", totalTime)

	if !this.replay {
		atomic.AddInt32(&writeBackFileCount, -1)
		atomic.AddInt64(&writeBackFileSize, -int64(n))
	}

}

func (this *sqlUpdater) run() {
	for {
		closed, localList := this.queue.Get()

		for _, v := range localList {
			if v.(int64) == -1 {
				if time.Now().Sub(this.lastTime) > time.Second*5*60 {
					//空闲超过5分钟发送ping
					err := this.db.Ping()
					if nil != err {
						Errorln("sqlUpdater ping error", err)
					}
					this.lastTime = time.Now()
				}
			} else {
				config := conf.GetConfig()
				this.process(fmt.Sprintf("%s/%s_%d.wb", config.WriteBackFileDir, config.WriteBackFilePrefix, v.(int64)))
			}
		}

		if closed {
			Infoln(this.name, "stoped")
			if nil != this.wg {
				this.wg.Done()
			}
			return
		}
	}
}
