package server

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

const (
	binlog_none     = 0
	binlog_snapshot = 1
	binlog_update   = 2
	binlog_delete   = 3
	binlog_kick     = 4
)

type batchBinlog struct {
	binlogStr *str
	ctxs      *ctxArray
	index     uint64
}

type commitedBatchBinlog struct {
	data []byte
	ctxs *ctxArray
	//	localPropose bool
	Index uint64
}

func (this *kvstore) tryProposeBatch() {

	if this.batchCount > 0 {

		config := conf.GetConfig()

		if this.batchCount >= int32(config.FlushCount) || this.binlogStr.dataLen() >= config.FlushSize || time.Now().After(this.nextFlush) {

			this.batchCount = 0

			binlogStr := this.binlogStr
			ctxs := this.ctxs

			this.binlogStr = nil
			this.ctxs = nil

			binary.BigEndian.PutUint64(binlogStr.data[:8], uint64(0))

			this.Propose(&batchBinlog{
				binlogStr: binlogStr,
				ctxs:      ctxs,
			})
		}
	}
}

func (this *kvstore) appendBinLog(tt int, unikey string, fields map[string]*proto.Field, version int64) {

	if nil == this.binlogStr {
		this.binlogStr = strGet()
		this.binlogStr.appendInt64(0)
	}

	this.batchCount++

	if this.batchCount == 1 {
		this.nextFlush = time.Now().Add(time.Millisecond * time.Duration(conf.GetConfig().FlushInterval))
	}

	this.binlogStr.appendBinLog(tt, unikey, fields, version)
}

func fillDefaultValue(meta *table_meta, ctx *cmdContext) {
	for _, v := range meta.fieldMetas {
		defaultV := proto.PackField(v.name, v.defaultV)
		if _, ok := ctx.fields[v.name]; !ok {
			ctx.fields[v.name] = defaultV
		}
	}
}

func (this *kvstore) checkContext(ckey *cacheKey, ctx *cmdContext) (bool, int) {
	ckey.mtx.Lock()

	gotErr := false
	binop := binlog_none
	sqlFlag := write_back_none

	switch ckey.sqlFlag {
	case write_back_none:
		sqlFlag = ctx.writeBackFlag
	case write_back_insert, write_back_insert_update:
		if ctx.writeBackFlag == write_back_update {
			sqlFlag = write_back_insert_update
		} else if ctx.writeBackFlag == write_back_delete {
			sqlFlag = write_back_delete
		} else {
			gotErr = true
			Errorln("invaild ctx.writeBackFlag")
		}
	case write_back_delete:
		if ctx.writeBackFlag == write_back_insert {
			sqlFlag = write_back_insert
		} else {
			gotErr = true
			Errorln("invaild ctx.writeBackFlag")
		}
	case write_back_update:
		if ctx.writeBackFlag == write_back_update {
			sqlFlag = write_back_update
		} else if ctx.writeBackFlag == write_back_delete {
			sqlFlag = write_back_delete
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
		return false, binop
	} else {

		cmdType := ctx.getCmdType()

		if cmdType != cmdDel && nil == ckey.values {
			fillDefaultValue(ckey.getMeta(), ctx)
		}

		switch cmdType {
		case cmdIncrBy, cmdDecrBy:
			cmd := ctx.getCmd()
			var newV *proto.Field
			var oldV *proto.Field
			if nil != ckey.values {
				oldV = ckey.values[cmd.incrDecr.GetName()]
			} else {
				oldV = ctx.fields[cmd.incrDecr.GetName()]
			}
			if cmdType == cmdIncrBy {
				newV = proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()+cmd.incrDecr.GetInt())
			} else {
				newV = proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()-cmd.incrDecr.GetInt())
			}
			ctx.fields[newV.GetName()] = newV
			ctx.version = ckey.version + 1
		case cmdDel:
			ctx.version = 0
		case cmdSet, cmdSetNx, cmdCompareAndSet, cmdCompareAndSetNx:
			ctx.version = ckey.version + 1
		}

		switch sqlFlag {
		case write_back_delete:
			if ckey.snapshoted {
				binop = binlog_delete
			} else {
				binop = binlog_snapshot
			}
		case write_back_insert, write_back_insert_update:
			binop = binlog_snapshot
		case write_back_update:
			if ckey.snapshoted {
				binop = binlog_update
			} else {
				binop = binlog_snapshot
			}
		}

		if binop == binlog_snapshot && sqlFlag != write_back_delete && ckey.values != nil {
			for k, v := range ckey.values {
				if _, ok := ctx.fields[k]; !ok {
					ctx.fields[k] = v
				}
			}
		}

		ckey.mtx.Unlock()
		ctx.writeBackFlag = sqlFlag
		return true, binop
	}
}

func (this *kvstore) issueUpdate(ctx *cmdContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	Debugln("writeBack")

	ckey := ctx.getCacheKey()

	ok, binop := this.checkContext(ckey, ctx)

	if !ok {
		return
	}

	this.mtx.Lock()

	if nil == this.ctxs {
		this.ctxs = ctxArrayGet()
	}

	this.ctxs.append(ctx)

	if len(ctx.fields) == 0 || ctx.version == 0 {
		panic("len(ctx.fields == 0) || ctx.version == 0")
	}

	this.appendBinLog(binop, ckey.uniKey, ctx.fields, ctx.version)

	this.tryProposeBatch()

	this.mtx.Unlock()

}
