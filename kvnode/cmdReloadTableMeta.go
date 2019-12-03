package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

func reloadTableMeta(n *KVNode, cli *cliConn, msg *codec.Message) {
	dbMetaStr, err := loadMetaString()
	head := msg.GetHead()
	errStr := ""

	if nil == err {
		err = n.storeMgr.dbmeta.Reload(dbMetaStr)
		if nil != err {
			head.ErrCode = errcode.ERR_OTHER
			errStr = err.Error()
		} else {
			head.ErrCode = errcode.ERR_OK
		}
	} else {
		head.ErrCode = errcode.ERR_OTHER
		errStr = err.Error()
	}

	cli.send(codec.NewMessage("", head, &proto.ReloadTableConfResp{Err: errStr}))

}
