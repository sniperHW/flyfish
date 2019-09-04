package server

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/proto"
	"hash/crc64"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *  binlog重放
 */

func getFileList(dirpath string) ([]string, error) {
	var file_list []string
	dir_err := filepath.Walk(dirpath,
		func(path string, f os.FileInfo, err error) error {
			if f == nil {
				return err
			}
			if !f.IsDir() {
				file_list = append(file_list, path)
				return nil
			}

			return nil
		})
	return file_list, dir_err
}

type ByID []string

func (a ByID) Len() int      { return len(a) }
func (a ByID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByID) Less(i, j int) bool {
	l := a[i]
	r := a[j]
	fieldl := strings.Split(l, "_")
	fieldr := strings.Split(r, "_")
	if len(fieldl) != 2 || len(fieldr) != 2 {
		panic("invaild writeBack file")
	}

	idl, err := strconv.ParseInt(strings.TrimRight(fieldl[1], binlogSuffix), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	idr, err := strconv.ParseInt(strings.TrimRight(fieldr[1], binlogSuffix), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	return idl < idr
}

func sortFileList(fileList []string) {
	sort.Sort(ByID(fileList))
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

func replay(recordCount int, path string, begOffset int, tt int, unikey string, version int64, values map[string]*proto.Field) bool {
	m := getKvstore(unikey)
	m.mtx.Lock()
	defer m.mtx.Unlock()
	ckey, _ := m.kv[unikey]
	if tt == binlog_snapshot {
		if nil == ckey {
			tmp := strings.Split(unikey, ":")
			ckey = newCacheKey(m, tmp[0], strings.Join(tmp[1:], ""), unikey)
			m.kv[unikey] = ckey
			m.updateLRU(ckey)
		}
		ckey.values = values
		ckey.version = version
		if ckey.version == 0 {
			ckey.sqlFlag = write_back_delete
			ckey.status = cache_missing
		} else {
			ckey.sqlFlag = write_back_insert_update
			ckey.status = cache_ok
		}
	} else if tt == binlog_update {
		if ckey != nil {
			if ckey.status != cache_ok || ckey.values == nil {
				Fatalln("invaild tt", path, unikey, tt, recordCount, begOffset)
				return false
			}
			for k, v := range values {
				ckey.values[k] = v
			}
			ckey.version = version
			ckey.sqlFlag = write_back_insert_update
		}
	} else if tt == binlog_kick {
		if ckey != nil {
			m.removeLRU(ckey)
			delete(m.kv, unikey)
		}
	} else {
		Fatalln("invaild tt", path, unikey, tt, recordCount, begOffset)
		return false
	}

	return true
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
		begOffset := totalOffset
		size := int(binary.BigEndian.Uint32(buffer[totalOffset : totalOffset+4]))
		totalOffset += 4
		checkSum := binary.BigEndian.Uint64(buffer[totalOffset : totalOffset+checkSumSize])
		totalOffset += checkSumSize
		//校验数据
		if checkSum != crc64.Checksum(buffer[totalOffset:totalOffset+size], crc64Table) {
			Fatalln("checkSum failed:", path, begOffset)
			return false
		}

		offset := totalOffset
		end := totalOffset + size
		totalOffset += size

		for offset < end {
			begOffset := offset
			newOffset, tt, unikey, version, values := readBinLog(buffer, offset)
			offset = newOffset
			recordCount++
			if !replay(recordCount, path, begOffset, tt, unikey, version, values) {
				return false
			}
		}
	}

	totalTime := time.Now().Sub(beg)

	Infoln("loadTime:", loadTime, "recordCount:", recordCount, "totalTime:", totalTime)

	return true
}

func (this *kvstore) firstSnapshot(config *conf.Config, wg *sync.WaitGroup) {

	beg := time.Now()

	fileIndex := atomic.AddInt64(&fileCounter, 1)
	os.MkdirAll(config.BinlogDir, os.ModePerm)
	path := fmt.Sprintf("%s/%s_%d%s", config.BinlogDir, config.BinlogPrefix, fileIndex, binlogSuffix)

	f, err := os.Create(path)
	if err != nil {
		Fatalln("create backfile failed", path, err)
	}

	this.binlogStr = strGet()

	this.binlogCount = 0

	for _, v := range this.kv {
		v.mtx.Lock()
		v.snapshoted = true
		this.appendBinlog(binlog_snapshot, v.uniKey, v.values, v.version)

		/*
		 *  如果某个key的binlog在序列化到磁盘后回写到sql前进程崩溃，且这个key长时间不被访问，
		 *  对于这个key的内容flyfish和sql将长时间不一致，为了避免这种情况，强制执行一次sql回写。
		 */
		if v.status == cache_ok {
			v.sqlFlag = write_back_insert_update
		} else {
			v.sqlFlag = write_back_delete
		}
		v.writeBackLocked = true
		v.mtx.Unlock()
		pushSqlWriteReq(v)
	}

	if this.binlogCount > 0 {
		head := make([]byte, 4+checkSumSize)
		checkSum := crc64.Checksum(this.binlogStr.bytes(), crc64Table)
		binary.BigEndian.PutUint32(head[0:4], uint32(this.binlogStr.dataLen()))
		binary.BigEndian.PutUint64(head[4:], uint64(checkSum))

		if _, err := f.Write(head); nil != err {
			onWriteFileError(err)
		}

		if _, err := f.Write(this.binlogStr.bytes()); nil != err {
			onWriteFileError(err)
		}

		if err := f.Sync(); nil != err {
			onWriteFileError(err)
		}
	}

	this.f = f
	this.filePath = path
	this.fileSize = this.binlogStr.dataLen()
	this.batchCount = 0

	this.binlogStr.reset()

	Infoln("snapshot time:", time.Now().Sub(beg), " count:", this.binlogCount)

	wg.Done()

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

	if len(fileList) > 0 {
		//记录下最大的文件序号
		max := fileList[len(fileList)-1]
		tmp := strings.Split(max, "_")
		id, _ := strconv.ParseInt(strings.TrimRight(tmp[1], binlogSuffix), 10, 64)
		fileCounter = id
	}

	for _, v := range fileList {
		if !replayBinLog(v) {
			return false
		}
	}

	wg := &sync.WaitGroup{}

	totalKvCount := 0

	//建立新快照
	for _, v := range kvstoreMgr {
		wg.Add(1)
		go func(m *kvstore) {
			m.mtx.Lock()
			m.firstSnapshot(config, wg)
			m.mtx.Unlock()
		}(v)
	}

	wg.Wait()

	//重放完成删除所有文件
	for _, v := range fileList {
		os.Remove(v)
	}

	for _, v := range kvstoreMgr {
		totalKvCount += len(v.kv)
	}

	Infoln("totalKvCount:", totalKvCount)

	return true
}

func binlogTypeToString(tt int) string {
	switch tt {
	case binlog_snapshot:
		return "binlog_snapshot"
	case binlog_update:
		return "binlog_update"
	case binlog_kick:
		return "binlog_kick"
	default:
		return "unkonw"
	}
}

func binlogDetail(fields map[string]*proto.Field) {
	for k, v := range fields {
		switch v.GetType() {
		case proto.ValueType_string:
			fmt.Println(k, v.GetString())
		case proto.ValueType_blob:
			fmt.Println(k, v.GetBlob())
		case proto.ValueType_float:
			fmt.Println(k, v.GetFloat())
		case proto.ValueType_int:
			fmt.Println(k, v.GetInt())
		case proto.ValueType_uint:
			fmt.Println(k, v.GetUint())
		default:
			panic("invaild field type")
		}
	}
}

func ShowBinlog(path string, showDetail bool) {

	stat, err := os.Stat(path)

	if nil != err {
		fmt.Println(err)
		return
	}

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

		c := 0
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
				newOffset, tt, unikey, version, fields := readBinLog(buffer, offset)
				c++
				offset = newOffset
				fmt.Println(c, unikey, "version:", version, "type:", binlogTypeToString(tt))
				if showDetail {
					binlogDetail(fields)
					fmt.Println()
				}
			}
		}
		return true
	}

	if stat.IsDir() {

		//获得所有文件
		fileList, err := getFileList(path)
		if nil != err {
			return
		}

		//对fileList排序
		sortFileList(fileList)

		for _, v := range fileList {
			if !read(v) {
				return
			}
		}
	} else {
		read(path)
	}
}
