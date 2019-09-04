package server

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"hash/crc64"
	"os"
	"sync/atomic"
	"time"
)

/*
 *  binlog操作只有三种合法类型
 *  snapshot:kv的全量状态信息，如果key被删除,binlog中fields信息为空,version==0
 *  update:基于前一个snapshot的fields变更以及最新版本号
 *  kick:将对应key踢除缓存
 *
 *  binlog文件的组织:
 *  对于binlog中存在kv日志，每个kv相关日志必定以一个snapshot开始。
 *
 *
 *  当内存中新插入一个kv对时,当前binlog文件中不存在kv的snapshot,此时kv的snapshoted标记为false。
 *  这个kv在当前binlog文件中的第一条log将以snapshot标记写入。写入后snapshoted标记设置true,后续更新
 *  update操作将以update标记写入binlog文件。
 *
 *  binlog文件替换:
 *
 *  当前binlog文件超过设定的条目数量后，将触发binlog替换流程。
 *
 *  1）记录下旧binlog文件的path.
 *  2) 创建新的binlog文件设置为当前binlog文件。将当前所有kv保存到一个数组中，将每个kv的snapshoted标记设置为false。
 *  3）启动独立的go程,遍历kv数组，向binlog文件追加snapshot,追加完成后kv的snapshoted标记设置为true。
 *  4）执行完且持久化到磁盘后删除旧的binlog文件。
 *
 *  优化:
 *  遍历与主服务是并行执行的。因此可能在处理某个kv之前，这个kv接收到了新的update操作请求。因为此时snapshoted标记为false,
 *  所以会向当前binlog插入snapshot日志并将snapshoted设置为true，当遍历到这个kv时，发现snapshoted为true,则无需再重复写入snapshot。
 */

const (
	binlog_none     = 0
	binlog_snapshot = 1
	binlog_update   = 2
	binlog_kick     = 3
)

type binlogBatch struct {
	binlogStr  *str
	ctxs       *ctxArray
	batchCount int32
}

var (
	fileCounter  int64
	checkSumSize = 8
	crc64Table   *crc64.Table
	binlogSuffix = ".bin"
	tmpFileName  string
)

func onWriteFileError(err error) {
	//写文件错误可能是因为磁盘满导致，所以先删除预留文件，释放磁盘空间用来写日志
	os.Remove(tmpFileName)
	Errorln("onWriteFileError", err)
	os.Exit(1)
}

func (this *kvstore) startSnapshot() {

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

	this.binlogStr = strGet()

	this.binlogCount = 0
	this.fileSize = 0

	this.f = f
	this.filePath = path

	kv := []*kv{}

	for _, v := range this.kv {
		v.mtx.Lock()
		if v.status == cache_ok || v.status == cache_missing {
			v.snapshoted = false
			v.make_snapshot = true
			kv = append(kv, v)
		}
		v.mtx.Unlock()
	}

	go func() {
		beg := time.Now()
		Infoln("start snapshot")
		c := 0
		i := 0
		for _, v := range kv {
			this.mtx.Lock()
			v.mtx.Lock()
			if (v.status == cache_ok || v.status == cache_missing) && !v.snapshoted {
				c++
				v.snapshoted = true
				this.appendBinlog(binlog_snapshot, v.uniKey, v.values, v.version)

			}
			v.make_snapshot = false
			v.mtx.Unlock()
			this.mtx.Unlock()
			i++
			if i%100 == 0 {
				time.Sleep(time.Millisecond * 10)
			}
		}

		backFilePath := this.backFilePath

		this.mtx.Lock()
		this.make_snapshot = false
		this.mtx.Unlock()
		Infoln("snapshot ok", time.Now().Sub(beg), c)

		this.binlogQueue.AddNoWait(func() {
			//删除backfile
			os.Remove(backFilePath)
		})

	}()
}

func (this *kvstore) batchWriteBinlog(binlogStr *str, ctxs *ctxArray, batchCount int32) {
	this.mtx.Lock()

	beg := time.Now()

	config := conf.GetConfig()

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
	checkSum := crc64.Checksum(binlogStr.bytes(), crc64Table)
	binary.BigEndian.PutUint32(head[0:4], uint32(binlogStr.dataLen()))
	binary.BigEndian.PutUint64(head[4:], uint64(checkSum))

	this.fileSize += binlogStr.dataLen() + len(head)

	this.mtx.Unlock()

	if _, err := this.f.Write(head); nil != err {
		onWriteFileError(err)
	}

	if _, err := this.f.Write(binlogStr.bytes()); nil != err {
		onWriteFileError(err)
	}

	if err := this.f.Sync(); nil != err {
		onWriteFileError(err)
	}

	this.mtx.Lock()

	if this.binlogCount >= config.MaxBinlogCount { // || this.fileSize >= int(config.MaxBinlogFileSize) {
		this.startSnapshot()
	}

	Debugln("flush time:", time.Now().Sub(beg), batchCount)

	this.mtx.Unlock()

	if nil != ctxs {
		for i := 0; i < ctxs.count; i++ {
			v := ctxs.ctxs[i]
			v.reply(errcode.ERR_OK, v.fields, v.version)
			ckey := v.getCacheKey()
			ckey.mtx.Lock()
			if !ckey.writeBackLocked {
				ckey.writeBackLocked = true
				pushSqlWriteReq(ckey)
			}
			ckey.mtx.Unlock()
		}

		for i := 0; i < ctxs.count; i++ {
			v := ctxs.ctxs[i]
			v.getCacheKey().processQueueCmd()
		}
		ctxArrayPut(ctxs)
	}
	strPut(binlogStr)
}

func (this *kvstore) tryBatchWrite() {

	if this.batchCount > 0 {

		config := conf.GetConfig()

		if this.batchCount >= int32(config.FlushCount) || this.binlogStr.dataLen() >= config.FlushSize || time.Now().After(this.nextFlush) {

			batchCount := this.batchCount

			this.batchCount = 0

			binlogStr := this.binlogStr
			ctxs := this.ctxs

			this.binlogStr = nil
			this.ctxs = nil

			this.binlogQueue.AddNoWait(&binlogBatch{
				binlogStr:  binlogStr,
				ctxs:       ctxs,
				batchCount: batchCount,
			})
		}
	}
}

func (this *kvstore) appendBinlog(tt int, unikey string, fields map[string]*proto.Field, version int64) {

	if nil == this.binlogStr {
		this.binlogStr = strGet()
	}

	this.binlogCount++
	this.batchCount++

	if this.batchCount == 1 {
		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(conf.GetConfig().FlushInterval))
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
		if nil != fields {
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
	} else {
		this.binlogStr.appendInt32(int32(0))
	}
}

func (this *kvstore) checkCacheKey(ckey *kv, ctx *cmdContext) bool {
	ckey.mtx.Lock()

	gotErr := false

	switch ckey.sqlFlag {
	case write_back_none:
		ckey.sqlFlag = ctx.writeBackFlag
	case write_back_insert, write_back_insert_update:
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_insert_update
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			gotErr = true
			Errorln("invaild ctx.writeBackFlag")
		}
	case write_back_delete:
		if ctx.writeBackFlag == write_back_insert {
			ckey.sqlFlag = write_back_insert
		} else {
			gotErr = true
			Errorln("invaild ctx.writeBackFlag")
		}
	case write_back_update:
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_update
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			gotErr = true
			Errorln("invaild ctx.writeBackFlag")
		}
	default:
		gotErr = true
		Errorln("invaild ctx.writeBackFlag")
	}

	if gotErr {
		ckey.mtx.Unlock()
		ctx.reply(errcode.ERR_ERROR, nil, -1)
		ckey.processQueueCmd()
		return false
	} else {

		cmdType := ctx.getCmdType()

		if cmdType != cmdDel && nil == ckey.values {
			ckey.setDefaultValueNoLock()
		}

		switch cmdType {
		case cmdIncrBy, cmdDecrBy:
			cmd := ctx.getCmd()
			var newV *proto.Field
			oldV := ckey.values[cmd.incrDecr.GetName()]
			if cmdType == cmdIncrBy {
				newV = proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()+cmd.incrDecr.GetInt())
			} else {
				newV = proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()-cmd.incrDecr.GetInt())
			}
			ckey.modifyFields[newV.GetName()] = true
			ckey.values[newV.GetName()] = newV
			ctx.fields[newV.GetName()] = newV
			ckey.setOKNoLock(ckey.version + 1)
		case cmdDel:
			ckey.setMissingNoLock()
		case cmdSet, cmdSetNx, cmdCompareAndSet, cmdCompareAndSetNx:
			for k, v := range ctx.fields {
				if k != "__version__" {
					ckey.values[k] = v
					ckey.modifyFields[k] = true
				}
			}
			ckey.setOKNoLock(ckey.version + 1)
		}

		ctx.version = ckey.version

		ckey.mtx.Unlock()
		return true
	}
}

func (this *kvstore) processUpdate(ctx *cmdContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	ckey := ctx.getCacheKey()

	if !this.checkCacheKey(ckey, ctx) {
		return
	}

	this.mtx.Lock()
	ckey.mtx.Lock()

	if nil == this.ctxs {
		this.ctxs = ctxArrayGet()
	}

	this.ctxs.append(ctx)

	switch ckey.sqlFlag {
	case write_back_delete:
		ckey.snapshoted = true
		this.appendBinlog(binlog_snapshot, ckey.uniKey, nil, 0)
	case write_back_insert, write_back_insert_update:
		ckey.snapshoted = true
		this.appendBinlog(binlog_snapshot, ckey.uniKey, ckey.values, ckey.version)
	case write_back_update:
		if ckey.snapshoted {
			this.appendBinlog(binlog_update, ckey.uniKey, ctx.fields, ckey.version)
		} else {
			ckey.snapshoted = true
			this.appendBinlog(binlog_snapshot, ckey.uniKey, ckey.values, ckey.version)
		}
	}

	ckey.mtx.Unlock()

	this.tryBatchWrite()

	this.mtx.Unlock()

}

func init() {
	crc64Table = crc64.MakeTable(crc64.ISO)
}
