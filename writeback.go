package flyfish

import (
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"time"
)

func (this *processUnit) flushBatch() *ctxArray {
	if this.ctxs != nil {
		err := levelDB.Write(this.levelDBBatch, nil)
		if err == nil {
			for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				ckey := v.getCacheKey()
				ckey.mtx.Lock()
				v.reply(errcode.ERR_OK, nil, ckey.version)
				if !ckey.writeBackLocked {
					ckey.writeBackLocked = true
					pushSqlWriteReq(ckey)
				}
				ckey.mtx.Unlock()
			}
		} else {
			for i := 0; i < this.ctxs.count; i++ {
				v := this.ctxs.ctxs[i]
				v.reply(errcode.ERR_LEVELDB, nil, -1)
			}
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
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), nil, 0)
	} else if ckey.sqlFlag == write_back_insert {
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ckey.values, ckey.version)
	} else {
		levelDBWrite(this.levelDBBatch, ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ctx.fields, ckey.version)
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
