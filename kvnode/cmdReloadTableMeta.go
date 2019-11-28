package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

func reloadTableMeta(n *KVNode, cli *cliConn, msg *codec.Message) {
	dbMetaStr, err := loadMetaString()
	req := msg.GetData().(*proto.ReloadTableConfReq)
	resp := &proto.ReloadTableConfResp{
		Seqno: req.Seqno,
	}
	if nil == err {
		err = n.storeMgr.dbmeta.Reload(dbMetaStr)
		if nil != err {
			resp.ErrCode = errcode.ERR_OTHER
			resp.Err = err.Error()
		} else {
			resp.ErrCode = errcode.ERR_OK
		}
	} else {
		resp.ErrCode = errcode.ERR_OTHER
		resp.Err = err.Error()
	}

	cli.send(resp)
}
