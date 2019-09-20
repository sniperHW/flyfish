package server

import (
	//"encoding/binary"
	//"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	//"time"
)

const (
	proposal_none     = 0
	proposal_snapshot = 1
	proposal_update   = 2
	proposal_delete   = 3
	proposal_kick     = 4
)

type proposal struct {
	ctx *cmdContext
	op  int
}

type batchProposal struct {
	proposalStr *str
	ctxs        *ctxArray
	index       int64
}

func (this *batchProposal) onCommit() *ctxArray {
	strPut(this.proposalStr)
	return this.ctxs
}

func (this *batchProposal) onError(err int) {
	for i := 0; i < this.ctxs.count; i++ {
		v := this.ctxs.ctxs[i]
		ckey := v.getCacheKey()
		if v.getCmdType() != cmdKick {
			v.reply(int32(err), nil, 0)
		} else {
			ckey.clearKicking()
		}

		if !ckey.tryRemoveTmpKey(err) {
			ckey.processQueueCmd()
		}
	}
	ctxArrayPut(this.ctxs)
	strPut(this.proposalStr)
}

func (this *batchProposal) onPorposeTimeout() {
	/*   timeout
	 *   只是对客户端超时,复制处理还在继续
	 *   所以只向客户端返回response
	 */
	for i := 0; i < this.ctxs.count; i++ {
		v := this.ctxs.ctxs[i]
		if v.getCmdType() != cmdKick {
			v.reply(errcode.ERR_TIMEOUT, nil, 0)
		}
		v.getCacheKey().processQueueCmd()
	}
}

type commitedBatchProposal struct {
	data []byte
	ctxs *ctxArray
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
	proposalOP := proposal_none
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
		return false, proposalOP
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
				proposalOP = proposal_delete
			} else {
				proposalOP = proposal_snapshot
			}
		case write_back_insert, write_back_insert_update:
			proposalOP = proposal_snapshot
		case write_back_update:
			if ckey.snapshoted {
				proposalOP = proposal_update
			} else {
				proposalOP = proposal_snapshot
			}
		}

		if proposalOP == proposal_snapshot && sqlFlag != write_back_delete && ckey.values != nil {
			for k, v := range ckey.values {
				if _, ok := ctx.fields[k]; !ok {
					ctx.fields[k] = v
				}
			}
		}

		ckey.mtx.Unlock()
		ctx.writeBackFlag = sqlFlag
		return true, proposalOP
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

	this.proposeC.AddNoWait(&proposal{
		op:  binop,
		ctx: ctx,
	})

}

/*
 *   向副本同步插入kv操作(从数据库load导致,数据内容并无变更)
 */

func (this *kvstore) issueAddKv(ctx *cmdContext) {

	this.proposeC.AddNoWait(&proposal{
		op:  proposal_snapshot,
		ctx: ctx,
	})
}
