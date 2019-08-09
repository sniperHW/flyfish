package flyfish

import (
	"encoding/binary"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/util"
	"hash/crc64"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *  回写先写入buffer,当buffer满或超过刷新时间，将buffer中的内容flush到磁盘
 *  成功后通知sqlupdater加载文件执行其中的回写操作，执行完成后删除文件
 */

var (
	fileCounter     int64
	binlogFileCount int32
	binlogFileSize  int64
	checkSumSize    = 8
	crc64Table      *crc64.Table
	binlogSuffix    = ".bin"
)

type ctxArray struct {
	count int
	ctxs  []*processContext
}

func (this *ctxArray) append(ctx *processContext) {
	this.ctxs[this.count] = ctx
	this.count++
}

func (this *ctxArray) full() bool {
	return this.count == cap(this.ctxs)
}

var ctxArrayPool = sync.Pool{
	New: func() interface{} {
		return &ctxArray{
			ctxs:  make([]*processContext, conf.GetConfig().FlushCount),
			count: 0,
		}
	},
}

func ctxArrayGet() *ctxArray {
	return ctxArrayPool.Get().(*ctxArray)
}

func ctxArrayPut(w *ctxArray) {
	w.count = 0
	ctxArrayPool.Put(w)
}

type binlogSt struct {
	binlogStr *str
	ctxs      *ctxArray
}

type writeBackProcessor struct {
	mtx            sync.Mutex
	nextFlush      time.Time
	nextChangeFile time.Time
	ctxs           *ctxArray
	sqlUpdater_    *sqlUpdater
	binlogQueue    *util.BlockQueue
	binlogStr      *str
	f              *os.File
	fileSize       int
	fileIndex      int64
}

func reachWriteBackFileLimit(config *conf.Config) bool {
	if atomic.LoadInt32(&binlogFileCount) > config.MaxBinlogFileCount {
		return true
	}

	if atomic.LoadInt64(&binlogFileSize) > config.MaxBinlogFileSize {
		return true
	}

	return false
}

func (this *writeBackProcessor) checkFlush() {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if time.Now().After(this.nextFlush) {
		this.flushToFile()
	}
}

func (this *writeBackProcessor) start() {
	this.binlogQueue = util.NewBlockQueue()
	this.ctxs = ctxArrayGet()
	go func() {
		for {
			closed, localList := this.binlogQueue.Get()
			for _, v := range localList {
				st := v.(*binlogSt)
				this.flush(st.binlogStr, st.ctxs)
			}
			if closed {
				return
			}
		}
	}()
}

func (this *writeBackProcessor) flush(s *str, ctxs *ctxArray) {

	config := conf.GetConfig()

	Debugln("flushToFile")

	if nil == this.f {

		this.fileIndex = atomic.AddInt64(&fileCounter, 1)
		atomic.AddInt32(&binlogFileCount, 1)

		os.MkdirAll(config.BinlogDir, os.ModePerm)
		path := fmt.Sprintf("%s/%s_%d%s", config.BinlogDir, config.BinlogPrefix, this.fileIndex, binlogSuffix)

		f, err := os.Create(path)
		if err != nil {
			Fatalln("create backfile failed", path, err)
			return
		}

		this.f = f
		this.nextChangeFile = time.Now().Add(time.Second)
	}

	if nil != s {

		head := make([]byte, 4+checkSumSize)
		checkSum := crc64.Checksum(s.bytes(), crc64Table)
		binary.BigEndian.PutUint32(head[0:4], uint32(s.dataLen()))
		binary.BigEndian.PutUint64(head[4:], uint64(checkSum))

		this.fileSize += s.dataLen() + len(head)
		this.f.Write(head)
		this.f.Write(s.bytes())
		atomic.AddInt64(&binlogFileSize, int64(s.dataLen()+len(head)))
		strPut(s)
		this.f.Sync()

	}

	if this.fileSize >= 1024*1024*4 || time.Now().After(this.nextChangeFile) {
		this.f.Close()
		//通告sqlUpdater执行更新
		this.sqlUpdater_.queue.AddNoWait(this.fileIndex)
		this.f = nil
		this.fileIndex = -1
		this.fileSize = 0
	}

	if nil != ctxs {
		for i := 0; i < ctxs.count; i++ {
			v := ctxs.ctxs[i]
			v.reply(errcode.ERR_OK, nil, v.fields["__version__"].GetInt())
		}
		for i := 0; i < ctxs.count; i++ {
			v := ctxs.ctxs[i]
			v.getCacheKey().processQueueCmd()
		}
		ctxArrayPut(ctxs)
	}

}

func (this *writeBackProcessor) flushToFile() {
	if this.binlogStr != nil {

		config := conf.GetConfig()

		st := &binlogSt{
			binlogStr: this.binlogStr,
			ctxs:      this.ctxs,
		}

		this.binlogStr = nil
		this.ctxs = ctxArrayGet()
		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval))

		this.binlogQueue.AddNoWait(st)
	} else if nil != this.f && time.Now().After(this.nextChangeFile) {
		st := &binlogSt{}
		this.binlogQueue.AddNoWait(st)
	}
}

func (this *writeBackProcessor) writeBack(ctx *processContext) {

	Debugln("writeBack")
	config := conf.GetConfig()

	var tt *proto.SqlType

	if ctx.writeBackFlag == write_back_insert {
		tt = proto.SqlType_insert.Enum()
	} else if ctx.writeBackFlag == write_back_update {
		tt = proto.SqlType_update.Enum()
	} else {
		tt = proto.SqlType_insert.Enum()
	}

	ckey := ctx.getCacheKey()
	ckey.mtx.Lock()
	ckey.writeBacked = true
	ckey.writeBackVersion++

	pbRecord := &proto.Record{
		Type:             tt,
		Table:            pb.String(ctx.getTable()),
		Key:              pb.String(ctx.getKey()),
		WritebackVersion: pb.Int64(ckey.writeBackVersion),
	}

	if ctx.writeBackFlag == write_back_update {
		pbRecord.Fields = make([]*proto.Field, len(ctx.fields))
		i := 0
		for _, v := range ctx.fields {
			pbRecord.Fields[i] = v
			i++
		}

	} else if ctx.writeBackFlag == write_back_insert {
		pbRecord.Fields = make([]*proto.Field, len(ctx.fields))
		meta := ctx.getCacheKey().getMeta()
		pbRecord.Fields[0] = ctx.fields["__version__"]
		i := 1
		for _, name := range meta.insertFieldOrder {
			if name != "__version__" {
				pbRecord.Fields[i] = ctx.fields[name]
				i++
			}
		}
	}

	ckey.mtx.Unlock()

	bytes, err := pb.Marshal(pbRecord)
	if err != nil {
		Fatalln("[marshalRecord]", err, *pbRecord)
		return
	}

	this.mtx.Lock()
	defer this.mtx.Unlock()

	if this.nextFlush.IsZero() {
		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval))
	}

	if nil == this.binlogStr {
		this.binlogStr = strGet()
	}

	this.binlogStr.appendInt32(int32(len(bytes)))
	this.binlogStr.appendBytes(bytes...)

	this.ctxs.append(ctx)

	if this.ctxs.full() || this.binlogStr.dataLen() >= config.FlushSize || time.Now().After(this.nextFlush) {
		this.flushToFile()
	}

	Debugln("writeBack ok")
}

func init() {
	crc64Table = crc64.MakeTable(crc64.ISO)
}
