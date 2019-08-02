package flyfish

import (
	"encoding/binary"
	"fmt"
	pb "github.com/golang/protobuf/proto"
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
	maxBufferSize   = 1024 * 1024 * 4
	fileDir         = "tmpWriteBackOp"
	filePrefix      = "tmpWriteBackOp"
	afterReplyQueue *util.BlockQueue
	crc64Table      *crc64.Table
)

func startAfterReplyProcess() {
	afterReplyQueue = util.NewBlockQueue()
	go func() {
		for {
			closed, localList := afterReplyQueue.Get()
			for _, v := range localList {
				for _, ctx := range v.([]*processContext) {
					ctx.getCacheKey().processQueueCmd()
				}
			}
			if closed {
				return
			}
		}
	}()
}

type writeBackProcessor struct {
	mtx          sync.Mutex
	buffer       []byte
	nextFlush    time.Time
	offset       int
	needReplys   []*processContext
	sqlUpdater_  *sqlUpdater
	checkSumBuff []byte
}

func (this *writeBackProcessor) checkFlush() {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.Now.After(this.nextFlush) {
		this.flushToFile()
	}
}

func (this *writeBackProcessor) flushToFile() {
	if this.offset > 0 {
		Debugln("flushToFile")
		counter := atomic.AddInt64(&fileCounter, 1)
		os.MkdirAll(fileDir, os.ModePerm)
		path := fmt.Sprintf("%s/%s_%d.op", fileDir, filePrefix, counter)

		f, err := os.Create(path)
		if err != nil {
			Fatalln("create backfile failed", path, err)
			return
		}
		checkSum := crc64.Checksum(this.buffer[:this.offset], crc64Table)
		f.Write(this.buffer[:this.offset])
		binary.BigEndian.PutUint64(this.checkSumBuff, checkSum)
		f.Write(this.checkSumBuff)
		this.offset = 0

		f.Sync()

		this.nextFlush = time.Now().Add(time.Duration(time.Millisecond * 100))

		//通告sqlUpdater执行更新

		this.sqlUpdater_.queue.AddNoWait(path)

		if len(this.needReplys) > 0 {
			for _, v := range this.needReplys {
				v.reply(errcode.ERR_OK, nil, v.fields["__version__"].GetInt())
			}
			afterReplyQueue.AddNoWait(this.needReplys)
			this.needReplys = []*processContext{}
		}
	}
}

func (this *writeBackProcessor) writeBack(ctx *processContext) {
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

	totalSize := 4 + len(bytes)

	this.mtx.Lock()
	defer this.mtx.Unlock()

	if this.nextFlush.IsZero() {
		this.nextFlush = time.Now().Add(time.Duration(time.Millisecond * 100))
	}

	if this.offset+totalSize > maxBufferSize {
		this.flushToFile()
		if totalSize > maxBufferSize {
			this.buffer = make([]byte, totalSize, totalSize)
		}
	}
	Debugln(this.offset)
	binary.BigEndian.PutUint32(this.buffer[this.offset:], uint32(len(bytes)))
	this.offset += 4
	copy(this.buffer[this.offset:], bytes)
	this.offset += len(bytes)

	if ctx.replyOnDbOk {
		this.needReplys = append(this.needReplys, ctx)
	}

	if this.offset+totalSize >= maxBufferSize || time.Now().After(this.nextFlush) {
		this.flushToFile()
	}
}

func init() {
	crc64Table = crc64.MakeTable(crc64.ISO)
}
