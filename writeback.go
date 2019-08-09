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
	fileCounter        int64
	writeBackFileCount int32
	writeBackFileSize  int64
	checkSumSize       = 8
	crc64Table         *crc64.Table
)

type writeFileSt struct {
	s          *str
	needReplys []*processContext
}

type writeBackProcessor struct {
	mtx            sync.Mutex
	nextFlush      time.Time
	nextChangeFile time.Time
	needReplys     []*processContext
	sqlUpdater_    *sqlUpdater
	writeFileQueue *util.BlockQueue
	s              *str
	checkSumStr    *str
	f              *os.File
	fileSize       int
	fileIndex      int64
	count          int
}

func reachWriteBackFileLimit(config *conf.Config) bool {
	if atomic.LoadInt32(&writeBackFileCount) > config.MaxWriteBackFileCount {
		return true
	}

	if atomic.LoadInt64(&writeBackFileSize) > config.MaxWriteBackFileSize {
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
	this.writeFileQueue = util.NewBlockQueue()
	go func() {
		for {
			closed, localList := this.writeFileQueue.Get()
			for _, v := range localList {
				st := v.(*writeFileSt)
				this.flush(st.s, st.needReplys)
			}
			if closed {
				return
			}
		}
	}()
}

var openWriteBack bool = true

func (this *writeBackProcessor) flush(s *str, needReplys []*processContext) {

	if openWriteBack {

		config := conf.GetConfig()

		Debugln("flushToFile")

		if nil == this.f {

			this.fileIndex = atomic.AddInt64(&fileCounter, 1)
			atomic.AddInt32(&writeBackFileCount, 1)

			os.MkdirAll(config.WriteBackFileDir, os.ModePerm)
			path := fmt.Sprintf("%s/%s_%d.wb", config.WriteBackFileDir, config.WriteBackFilePrefix, this.fileIndex)

			f, err := os.Create(path)
			if err != nil {
				Fatalln("create backfile failed", path, err)
				return
			}

			this.f = f
			this.nextChangeFile = time.Now().Add(time.Second)
		}

		if nil != s {

			if nil == this.checkSumStr {
				this.checkSumStr = strGet()
			}
			this.checkSumStr.appendBytes(s.bytes()...)

			this.fileSize += s.dataLen()
			this.f.Write(s.bytes())
			atomic.AddInt64(&writeBackFileSize, int64(s.dataLen()))
			strPut(s)
			this.f.Sync()

		}

		if this.fileSize >= 1024*1024*4 || time.Now().After(this.nextChangeFile) {

			checkSum := crc64.Checksum(this.checkSumStr.bytes(), crc64Table)
			checkSumBuffer := make([]byte, 8)
			binary.BigEndian.PutUint64(checkSumBuffer, uint64(checkSum))
			this.f.Write(checkSumBuffer)
			this.checkSumStr.reset()

			this.f.Close()
			//通告sqlUpdater执行更新
			this.sqlUpdater_.queue.AddNoWait(this.fileIndex)
			this.f = nil
			this.fileIndex = -1
			this.fileSize = 0
		}

		if len(needReplys) > 0 {
			for _, v := range needReplys {
				v.reply(errcode.ERR_OK, nil, v.fields["__version__"].GetInt())
			}
			for _, v := range needReplys {
				v.getCacheKey().processQueueCmd()
			}
		}

	} else {
		if nil != s {
			strPut(s)
		}
		if len(needReplys) > 0 {
			for _, v := range needReplys {
				v.reply(errcode.ERR_OK, nil, v.fields["__version__"].GetInt())
			}
			for _, v := range needReplys {
				v.getCacheKey().processQueueCmd()
			}
		}
	}
}

func (this *writeBackProcessor) flushToFile() {
	if this.s != nil {

		config := conf.GetConfig()

		st := &writeFileSt{
			s:          this.s,
			needReplys: this.needReplys,
		}

		this.s = nil
		this.needReplys = []*processContext{}
		this.count = 0
		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval))

		this.writeFileQueue.AddNoWait(st)
	} else if nil != this.f && time.Now().After(this.nextChangeFile) {
		st := &writeFileSt{
			needReplys: []*processContext{},
		}
		this.writeFileQueue.AddNoWait(st)
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

	if nil == this.s {
		this.s = strGet()
	}

	this.s.appendInt32(int32(len(bytes)))
	this.s.appendBytes(bytes...)
	this.count++

	this.needReplys = append(this.needReplys, ctx)

	if this.count >= config.FlushCount || this.s.dataLen() >= config.FlushSize || time.Now().After(this.nextFlush) {
		this.flushToFile()
	}

	Debugln("writeBack ok")
}

func init() {
	crc64Table = crc64.MakeTable(crc64.ISO)
}
