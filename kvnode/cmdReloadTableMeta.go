package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
)

func reloadTableMeta(n *KVNode, cli *cliConn, msg *net.Message) {
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

	cli.send(net.NewMessage(head, &proto.ReloadTableConfResp{Err: errStr}))

}
