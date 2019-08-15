package flyfish

import (
	//"encoding/binary"
	//"fmt"
	//pb "github.com/golang/protobuf/proto"
	//"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	//"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet/util"
	//"hash/crc64"
	//"os"
	//"sync"
	//"sync/atomic"
	//"time"
)

func writeBack(ctx *processContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	Debugln("writeBack")
	//config := conf.GetConfig()

	ckey := ctx.getCacheKey()

	ckey.mtx.Lock()
	version := ckey.version

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

	var err error

	if ckey.sqlFlag == write_back_delete {
		err = levelDBWrite(ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), nil)
	} else if ckey.sqlFlag == write_back_insert {
		err = levelDBWrite(ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ckey.values)
	} else {
		err = levelDBWrite(ckey.sqlFlag, ckey.uniKey, ckey.getMeta(), ctx.fields)
	}

	if nil != err {
		ckey.mtx.Unlock()

		ctx.reply(errcode.ERR_LEVELDB, nil, -1)

		Errorln(err)
	} else {

		if !ckey.writeBackLocked {
			ckey.writeBackLocked = true
			pushSqlWriteReq(ckey)
		}

		ckey.mtx.Unlock()

		ctx.reply(errcode.ERR_OK, nil, version)
	}

	ckey.processQueueCmd()
}
