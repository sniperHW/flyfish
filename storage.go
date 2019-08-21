package flyfish

import (
	"encoding/binary"
	"fmt"
	//"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet/util"
	"hash/crc64"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	binlog_snapshot = 1
	binlog_update   = 2
	binlog_delete   = 3
	binlog_kick     = 4
)

var (
	fileCounter  int64
	checkSumSize = 8
	crc64Table   *crc64.Table
	binlogSuffix = ".bin"
)

func binlogTypeToString(tt int) string {
	switch tt {
	case binlog_snapshot:
		return "binlog_snapshot"
	case binlog_update:
		return "binlog_update"
	case binlog_delete:
		return "binlog_delete"
	case binlog_kick:
		return "binlog_kick"
	default:
		return "unkonw"
	}
}

func (this *processUnit) startSnapshot() {

	if this.make_snapshot {
		return
	}

	config := conf.GetConfig()

	this.make_snapshot = true

	this.backFilePath = this.filePath
	this.f.Close()

	fileIndex := atomic.AddInt64(&fileCounter, 1)
	os.MkdirAll(config.BinlogDir, os.ModePerm)
	path := fmt.Sprintf("%s/%s_%d%s", config.BinlogDir, config.BinlogPrefix, fileIndex, binlogSuffix)

	f, err := os.Create(path)
	if err != nil {
		Fatalln("create backfile failed", path, err)
	}

	if nil != this.binlogStr {
		this.binlogStr.reset()
	} else {
		this.binlogStr = strGet()
	}

	this.binlogCount = 0
	this.fileSize = 0

	this.f = f
	this.filePath = path

	cacheKeys := []*cacheKey{}

	for _, v := range this.cacheKeys {
		v.mtx.Lock()
		if v.status == cache_ok || v.status == cache_missing {
			v.snapshot = false
			cacheKeys = append(cacheKeys, v)
		}
		v.mtx.Unlock()
	}

	go func() {
		beg := time.Now()
		Infoln("start snapshot")
		c := 0
		i := 0
		for _, v := range cacheKeys {
			this.mtx.Lock()
			v.mtx.Lock()
			if (v.status == cache_ok || v.status == cache_missing) && !v.snapshot {
				c++
				v.snapshot = true
				this.write(binlog_snapshot, v.uniKey, v.values, v.version)

			}
			v.make_snapshot = false
			v.mtx.Unlock()
			this.tryFlush()
			this.mtx.Unlock()
			i++
			if i >= 100 {
				time.Sleep(time.Millisecond)
			}
		}

		//移除backfile
		os.Remove(this.backFilePath)

		this.mtx.Lock()
		this.make_snapshot = false
		this.mtx.Unlock()
		Infoln("snapshot ok", time.Now().Sub(beg), c)
	}()
}

func (this *processUnit) tryFlush() {

	if this.cacheBinlogCount > 0 && (this.cacheBinlogCount >= int32(conf.GetConfig().FlushCount) || time.Now().After(this.nextFlush)) {

		cacheBinlogCount := this.cacheBinlogCount

		this.cacheBinlogCount = 0

		config := conf.GetConfig()

		beg := time.Now()

		if nil == this.f {

			fileIndex := atomic.AddInt64(&fileCounter, 1)

			os.MkdirAll(config.BinlogDir, os.ModePerm)
			path := fmt.Sprintf("%s/%s_%d%s", config.BinlogDir, config.BinlogPrefix, fileIndex, binlogSuffix)

			f, err := os.Create(path)
			if err != nil {
				Fatalln("create backfile failed", path, err)
				return
			}

			this.f = f
			this.filePath = path
		}

		head := make([]byte, 4+checkSumSize)
		checkSum := crc64.Checksum(this.binlogStr.bytes(), crc64Table)
		binary.BigEndian.PutUint32(head[0:4], uint32(this.binlogStr.dataLen()))
		binary.BigEndian.PutUint64(head[4:], uint64(checkSum))

		this.fileSize += this.binlogStr.dataLen() + len(head)

		if _, err := this.f.Write(head); nil != err {
			panic(err)
		}

		if _, err := this.f.Write(this.binlogStr.bytes()); nil != err {
			panic(err)
		}

		if err := this.f.Sync(); nil != err {
			panic(err)
		}

		if this.binlogCount >= config.MaxBinlogCount || this.fileSize+this.binlogStr.dataLen()+len(head) >= int(config.MaxBinlogFileSize) {
			this.startSnapshot()
		} else {
			this.binlogStr.reset()
		}

		Debugln("flush time:", time.Now().Sub(beg), cacheBinlogCount)

		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval))

		if nil != this.ctxs {
			ctxs := this.ctxs
			this.ctxs = nil

			for i := 0; i < ctxs.count; i++ {
				v := ctxs.ctxs[i]
				ckey := v.getCacheKey()
				ckey.mtx.Lock()
				v.reply(errcode.ERR_OK, nil, ckey.version)
				if !ckey.writeBackLocked {
					ckey.writeBackLocked = true
					pushSqlWriteReq(ckey)
				}
				ckey.mtx.Unlock()
			}

			this.mtx.Unlock()

			for i := 0; i < ctxs.count; i++ {
				v := ctxs.ctxs[i]
				v.getCacheKey().processQueueCmd()
			}
			ctxArrayPut(ctxs)

			this.mtx.Lock()
		}
	}
}

func (this *processUnit) write(tt int, unikey string, fields map[string]*proto.Field, version int64) {

	this.binlogCount++
	this.cacheBinlogCount++

	if nil == this.binlogStr {
		this.binlogStr = strGet()
	}

	//写操作码1byte
	this.binlogStr.appendByte(byte(tt))
	//写unikey
	this.binlogStr.appendInt32(int32(len(unikey)))
	this.binlogStr.append(unikey)
	//写version
	this.binlogStr.appendInt64(version)
	if tt == binlog_snapshot || tt == binlog_update {
		pos := this.binlogStr.len
		this.binlogStr.appendInt32(int32(0))
		c := 0
		for n, v := range fields {
			if n != "__version__" {
				c++
				this.binlogStr.appendField(v)
			}
		}
		if c > 0 {
			binary.BigEndian.PutUint32(this.binlogStr.data[pos:pos+4], uint32(c))
		}
	}
}

func (this *processUnit) writeKick(unikey string) {
	this.mtx.Lock()
	this.write(binlog_kick, unikey, nil, 0)
	this.tryFlush()
	this.mtx.Unlock()
}

func (this *processUnit) snapshot(config *conf.Config, wg *sync.WaitGroup) {

	beg := time.Now()

	fileIndex := atomic.AddInt64(&fileCounter, 1)
	os.MkdirAll(config.BinlogDir, os.ModePerm)
	path := fmt.Sprintf("%s/%s_%d%s", config.BinlogDir, config.BinlogPrefix, fileIndex, binlogSuffix)

	f, err := os.Create(path)
	if err != nil {
		Fatalln("create backfile failed", path, err)
	}

	if nil != this.binlogStr {
		this.binlogStr.reset()
	} else {
		this.binlogStr = strGet()
	}

	this.f = f
	this.filePath = path
	this.fileSize = 0
	this.make_snapshot = true
	this.binlogCount = 0

	for _, v := range this.cacheKeys {
		v.mtx.Lock()
		if v.status == cache_ok || v.status == cache_missing {
			v.snapshot = true
			this.write(binlog_snapshot, v.uniKey, v.values, v.version)
		}
		v.mtx.Unlock()
	}

	this.tryFlush()

	//执行一次sql操作，防止数据不一致的情况
	for _, v := range this.cacheKeys {
		v.mtx.Lock()
		v.writeBackLocked = true
		pushSqlWriteReq(v)
		v.mtx.Unlock()
	}

	this.make_snapshot = false

	this.binlogStr.reset()

	Infoln("snapshot time:", time.Now().Sub(beg), " count:", this.binlogCount)

	wg.Done()

}

func (this *processUnit) writeBack(ctx *processContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	Debugln("writeBack")

	ckey := ctx.getCacheKey()

	this.mtx.Lock()
	ckey.mtx.Lock()

	if ckey.sqlFlag == write_back_none {
		ckey.sqlFlag = ctx.writeBackFlag
	} else if ckey.sqlFlag == write_back_insert {
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_insert_update
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			Errorln("invaild ctx.writeBackFlag")
		}
	} else if ckey.sqlFlag == write_back_delete {
		if ctx.writeBackFlag == write_back_insert {
			ckey.sqlFlag = write_back_insert
		} else {
			Errorln("invaild ctx.writeBackFlag")
		}
	} else if ckey.sqlFlag == write_back_update {
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_update
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			Errorln("invaild ctx.writeBackFlag")
		}
	}

	if nil == this.ctxs {
		this.ctxs = ctxArrayGet()
	}

	this.ctxs.append(ctx)

	if ckey.sqlFlag == write_back_delete {
		if ckey.snapshot {
			this.write(binlog_delete, ckey.uniKey, nil, 0)
		} else {
			ckey.snapshot = true
			this.write(binlog_snapshot, ckey.uniKey, ckey.values, ckey.version)
		}
	} else if ckey.sqlFlag == write_back_insert {
		ckey.snapshot = true
		this.write(binlog_snapshot, ckey.uniKey, ckey.values, ckey.version)
	} else {
		if ckey.snapshot {
			this.write(binlog_update, ckey.uniKey, ctx.fields, ckey.version)
		} else {
			ckey.snapshot = true
			this.write(binlog_snapshot, ckey.uniKey, ckey.values, ckey.version)
		}
	}

	ckey.mtx.Unlock()
	this.tryFlush()
	this.mtx.Unlock()
}

func readBinLog(buffer []byte, offset int) (int, int, string, int64, map[string]*proto.Field) {
	tt := int(buffer[offset])
	offset += 1
	l := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
	offset += 4
	uniKey := string(buffer[offset : offset+l])
	offset += l
	version := int64(binary.BigEndian.Uint64(buffer[offset : offset+8]))
	offset += 8

	var values map[string]*proto.Field

	valueSize := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
	offset += 4

	if valueSize > 0 {
		values = map[string]*proto.Field{}
		for i := 0; i < valueSize; i++ {
			l := int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
			offset += 4
			name := string(buffer[offset : offset+l])
			offset += l

			vType := proto.ValueType(int(buffer[offset]))
			offset += 1

			switch vType {
			case proto.ValueType_string:
				l = int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
				offset += 4
				values[name] = proto.PackField(name, string(buffer[offset:offset+l]))
				offset += l
			case proto.ValueType_float:
				u64 := binary.BigEndian.Uint64(buffer[offset : offset+8])
				values[name] = proto.PackField(name, math.Float64frombits(u64))
				offset += 8
			case proto.ValueType_int:
				values[name] = proto.PackField(name, int64(binary.BigEndian.Uint64(buffer[offset:offset+8])))
				offset += 8
			case proto.ValueType_uint:
				values[name] = proto.PackField(name, uint64(binary.BigEndian.Uint64(buffer[offset:offset+8])))
				offset += 8
			case proto.ValueType_blob:
				l = int(binary.BigEndian.Uint32(buffer[offset : offset+4]))
				offset += 4
				v := make([]byte, l)
				copy(v, buffer[offset:offset+l])
				values[name] = proto.PackField(name, v)
				offset += l
			default:
				panic("invaild value type")
			}
		}
	}

	return offset, tt, uniKey, version, values
}

func replayBinLog(path string) bool {
	beg := time.Now()

	var err error

	stat, err := os.Stat(path)

	if nil != err {
		Fatalln("open file failed:", path, err)
		return false
	}

	f, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)

	if nil != err {
		Fatalln("open file failed:", path, err)
		return false
	}

	buffer := make([]byte, int(stat.Size()))

	n, err := f.Read(buffer)

	f.Close()

	loadTime := time.Now().Sub(beg)

	if n != (int)(stat.Size()) {
		Fatalln("read file failed:", path, err)
		return false
	}

	totalOffset := 0
	recordCount := 0
	for totalOffset < n {
		size := int(binary.BigEndian.Uint32(buffer[totalOffset : totalOffset+4]))
		totalOffset += 4
		checkSum := binary.BigEndian.Uint64(buffer[totalOffset : totalOffset+checkSumSize])
		totalOffset += checkSumSize
		//校验数据
		if checkSum != crc64.Checksum(buffer[totalOffset:totalOffset+size], crc64Table) {
			Fatalln("checkSum failed:", path)
			return false
		}

		offset := totalOffset
		end := totalOffset + size
		totalOffset += size

		for offset < end {
			newOffset, tt, unikey, version, values := readBinLog(buffer, offset)
			offset = newOffset

			unit := getUnitByUnikey(unikey)
			ckey, _ := unit.cacheKeys[unikey]
			recordCount++

			if tt == binlog_snapshot {
				if nil == ckey {
					tmp := strings.Split(unikey, ":")
					ckey = newCacheKey(unit, tmp[0], strings.Join(tmp[1:], ""), unikey)
					ckey.values = values
					ckey.version = version
					if ckey.version == 0 {
						ckey.sqlFlag = write_back_delete
						ckey.status = cache_missing
					} else {
						ckey.sqlFlag = write_back_insert_update
						ckey.status = cache_ok
					}
					unit.cacheKeys[unikey] = ckey
					unit.updateLRU(ckey)
				} else {
					ckey.values = values
					ckey.version = version
					ckey.sqlFlag = write_back_insert_update
					ckey.status = cache_ok
				}
			} else if tt == binlog_update {
				if nil == ckey || ckey.status != cache_ok || ckey.values == nil {
					Fatalln("invaild tt")
					return false
				}
				for k, v := range values {
					ckey.values[k] = v
				}
				ckey.version = version
				ckey.sqlFlag = write_back_insert_update
			} else if tt == binlog_delete {
				if nil == ckey || ckey.status != cache_ok {
					Fatalln("invaild tt")
					return false
				}
				ckey.values = nil
				ckey.version = version
				ckey.status = cache_missing
				ckey.sqlFlag = write_back_delete
			} else if tt == binlog_kick {
				if nil == ckey {
					Fatalln("invaild tt")
					return false
				}
				unit.removeLRU(ckey)
				delete(unit.cacheKeys, unikey)
			} else {
				Fatalln("invaild tt", path, tt, offset)
				return false
			}
		}
	}

	totalTime := time.Now().Sub(beg)

	Infoln("loadTime:", loadTime, "recordCount:", recordCount, "totalTime:", totalTime)

	return true
}

//执行尚未完成的回写文件
func StartReplayBinlog() bool {
	config := conf.GetConfig()

	_, err := os.Stat(config.BinlogDir)

	if nil != err && os.IsNotExist(err) {
		return true
	}

	//获得所有文件
	fileList, err := getFileList(config.BinlogDir)
	if nil != err {
		return false
	}

	//对fileList排序
	sortFileList(fileList)

	for _, v := range fileList {
		if !replayBinLog(v) {
			return false
		}
	}

	//重放完成删除所有文件
	for _, v := range fileList {
		os.Remove(v)
	}

	wg := &sync.WaitGroup{}

	//建立新快照
	for _, v := range processUnits {
		wg.Add(1)
		go func(u *processUnit) {
			u.mtx.Lock()
			u.snapshot(config, wg)
			u.mtx.Unlock()
		}(v)
	}

	wg.Wait()

	return true
}

func ShowBinlog(path string) {

	//获得所有文件
	fileList, err := getFileList(path)
	if nil != err {
		return
	}

	//对fileList排序
	sortFileList(fileList)

	read := func(path string) bool {

		var err error

		stat, err := os.Stat(path)

		if nil != err {
			Fatalln("open file failed:", path, err)
			return false
		}

		f, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)

		if nil != err {
			Fatalln("open file failed:", path, err)
			return false
		}

		buffer := make([]byte, int(stat.Size()))

		n, err := f.Read(buffer)

		f.Close()

		if n != (int)(stat.Size()) {
			Fatalln("read file failed:", path, err)
			return false
		}

		if n == 0 {
			return true
		}

		fmt.Println("-------------------------", path, "---------------------------")

		totalOffset := 0
		for totalOffset < n {
			size := int(binary.BigEndian.Uint32(buffer[totalOffset : totalOffset+4]))
			totalOffset += 4
			checkSum := binary.BigEndian.Uint64(buffer[totalOffset : totalOffset+checkSumSize])
			totalOffset += checkSumSize
			//校验数据
			if checkSum != crc64.Checksum(buffer[totalOffset:totalOffset+size], crc64Table) {
				Fatalln("checkSum failed:", path)
				return false
			}

			offset := totalOffset
			end := totalOffset + size
			totalOffset += size

			for offset < end {
				newOffset, tt, unikey, version, _ := readBinLog(buffer, offset)
				offset = newOffset
				fmt.Println(unikey, "version:", version, "type:", binlogTypeToString(tt))
			}
		}
		return true
	}

	for _, v := range fileList {
		if !read(v) {
			return
		}
	}
}

func init() {
	crc64Table = crc64.MakeTable(crc64.ISO)
}
