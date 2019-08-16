package flyfish

import (
	//"encoding/binary"
	//"fmt"
	//pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	//"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet/util"
	//"hash/crc64"
	//"os"
	//"sync"
	//"sync/atomic"
	"time"
)

func (this *processUnit) flushBatch() *ctxArray {
	//if this.ctxs.len() > 0 {
	if this.ctxs != nil {
		err := levelDB.Write(this.levelDBBatch, nil)
		if err == nil {
			for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				v.reply(errcode.ERR_OK, nil, v.fields["__version__"].GetInt())
				ckey := v.getCacheKey()
				ckey.mtx.Lock()
				if !ckey.writeBackLocked {
					ckey.writeBackLocked = true
					pushSqlWriteReq(ckey)
				}
				ckey.mtx.Unlock()
			}
			/*for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				v.getCacheKey().processQueueCmd()
			}*/
		} else {
			for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				v.reply(errcode.ERR_LEVELDB, nil, -1)
			}
			/*for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				v.getCacheKey().processQueueCmd()
			}*/
		}
		this.levelDBBatch.Reset()
	}
	config := conf.GetConfig()
	this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval))

	ctxs := this.ctxs
	this.ctxs = nil

	return ctxs

}

func (this *processUnit) writeBack(ctx *processContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	Debugln("writeBack")

	ckey := ctx.getCacheKey()

	this.mtx.Lock()

	if nil == this.ctxs {
		this.ctxs = ctxArrayGet()
	}

	ckey.mtx.Lock()

	if ckey.sqlFlag == write_back_none {
		ckey.sqlFlag = ctx.writeBackFlag
	} else if ckey.sqlFlag == write_back_insert {
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_insert
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			panic("invaild ctx.writeBackFlag")
		}
	} else if ckey.sqlFlag == write_back_delete {
		if ctx.writeBackFlag == write_back_insert {
			ckey.sqlFlag = write_back_insert
		} else {
			panic("invaild ctx.writeBackFlag")
		}
	} else if ckey.sqlFlag == write_back_update {
		if ctx.writeBackFlag == write_back_update {
			ckey.sqlFlag = write_back_update
		} else if ctx.writeBackFlag == write_back_delete {
			ckey.sqlFlag = write_back_delete
		} else {
			panic("invaild ctx.writeBackFlag")
		}
	}

	if ckey.sqlFlag == write_back_delete {
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), nil)
	} else if ckey.sqlFlag == write_back_insert {
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ckey.values)
	} else {
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ctx.fields)
	}

	ckey.mtx.Unlock()

	this.ctxs.append(ctx)

	var ctxs *ctxArray

	if this.ctxs.full() || time.Now().After(this.nextFlush) {
		ctxs = this.flushBatch()
	}

	this.mtx.Unlock()

	if nil != ctxs {
		for i := 0; i < ctxs.count; i++ {
			v := ctxs.ctxs[i]
			v.getCacheKey().processQueueCmd()
		}
		ctxArrayPut(ctxs)
	}
}
