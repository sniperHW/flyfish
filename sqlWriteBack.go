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
	"time"
)

type sqlUpdater struct {
	db       *sqlx.DB
	name     string
	values   []interface{}
	lastTime time.Time
	queue    *util.BlockQueue
	wg       *sync.WaitGroup
}

func newSqlUpdater(db *sqlx.DB, name string, wg *sync.WaitGroup) *sqlUpdater {
	if nil != wg {
		wg.Add(1)
	}
	return &sqlUpdater{
		name:   name,
		values: []interface{}{},
		queue:  util.NewBlockQueueWithName(name, conf.GetConfig().SqlUpdateQueueSize),
		db:     db,
		wg:     wg,
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

func (this *sqlUpdater) resetValues() {
	this.values = this.values[0:0]
}

func (this *sqlUpdater) doInsert(r *proto.Record, meta *table_meta) error {

	Debugln("doInsert")

	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append(meta.insertPrefix).append(getInsertPlaceHolder(1)).append(getInsertPlaceHolder(2))
	this.values = append(this.values, r.GetKey(), r.Fields[0].GetValue())
	c := 2

	for i := 1; i < len(r.Fields); i++ {
		c++
		str.append(getInsertPlaceHolder(c))
		this.values = append(this.values, r.Fields[i].GetValue())
	}

	str.append(");")
	_, err := this.db.Exec(str.toString(), this.values...)

	return err
}

func (this *sqlUpdater) doUpdate(r *proto.Record) error {

	Debugln("doUpdate")

	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append("update ").append(r.GetTable()).append(" set ")
	i := 0
	for _, v := range r.Fields {
		this.values = append(this.values, v.GetValue())
		i++
		if i == 1 {
			str.append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
		} else {
			str.append(",").append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
		}
	}

	str.append(" where __key__ = '").append(r.GetKey()).append("';")
	_, err := this.db.Exec(str.toString(), this.values...)
	return err
}

func (this *sqlUpdater) doDelete(r *proto.Record) error {
	Debugln("doDelete")
	str := strGet()
	defer strPut(str)

	str.append("delete from ").append(r.GetTable()).append(" where __key__ = '").append(r.GetKey()).append("';")
	_, err := this.db.Exec(str.toString())
	return err
}

func (this *sqlUpdater) process(v interface{}) {

	Debugln("sqlUpdater process")

	beg := time.Now()

	path := fmt.Sprintf("%s/%s_%d.op", fileDir, filePrefix, v.(int64))

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

	buffer := getBuffer(int(stat.Size()))

	n, err := f.Read(buffer)

	f.Close()

	loadTime := time.Now().Sub(beg)

	if n != (int)(stat.Size()) {
		Fatalln("read file failed:", path, err)
		return
	}

	checkSum := binary.BigEndian.Uint64(buffer[n-8:])

	//校验数据
	if checkSum != crc64.Checksum(buffer[:n-8], crc64Table) {
		Fatalln("checkSum failed:", path)
		return
	}

	recordCount := 0

	offset := 0
	end := n - 8
	for offset < end {
		pbRecord := &proto.Record{}
		l := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
		offset += 4
		if err = pb.Unmarshal(buffer[offset:offset+l], pbRecord); err != nil {
			Fatalln("replayRecord error ,offset:", offset, err)
		}
		offset += l

		meta := getMetaByTable(pbRecord.GetTable())

		if nil == meta {
			Fatalln("replayRecord error invaild table ,offset:", offset-l, pbRecord.GetTable())
		}

		this.lastTime = time.Now()

		tt := pbRecord.GetType()
		if tt == proto.SqlType_insert {
			err = this.doInsert(pbRecord, meta)
		} else if tt == proto.SqlType_update {
			err = this.doUpdate(pbRecord)
		} else if tt == proto.SqlType_delete {
			err = this.doDelete(pbRecord)
		} else {
			Fatalln("replayRecord invaild tt,offset:", offset)
		}

		uniKey := fmt.Sprintf("%s:%s", pbRecord.GetTable(), pbRecord.GetKey())

		unit := getUnitByUnikey(uniKey)

		unit.mtx.Lock()
		k, ok := unit.cacheKeys[uniKey]
		unit.mtx.Unlock()
		if ok {
			k.clearWriteBack(pbRecord.GetWritebackVersion())
		}
		recordCount++

		//if nil == err {
		/*if wb.ctx != nil {
			sqlResponse.onSqlWriteBackResp(wb.ctx, errcode.ERR_OK)
		}*/
		//} else {
		//	if isRetryError(err) {
		//		Errorln("sqlUpdater exec error:", err)
		//		if isStop() {
		//			this.writeFileAndBreak = true
		//			return
		//		}
		//休眠一秒重试
		//		time.Sleep(time.Second)
		//	} else {
		//		Errorln("sqlUpdater exec error:", err)
		//	}
		//}

	}

	releasaeBuffer(buffer)

	sqlTime := time.Now().Sub(beg)

	//删除文件
	os.Remove(path)

	totalTime := time.Now().Sub(beg)

	Infoln("loadTime:", loadTime, "recordCount:", recordCount, "sqlTime:", sqlTime, "totalTime:", totalTime)

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
				this.process(v)
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
