package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

func decrBy(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdIncrDecr{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		field: req.GetField(),
	}

	if nil == op.field {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	table, key := head.SplitUniKey()

	if kv, err := n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	} else {

		op.kv = kv

		if !kv.meta.CheckField(op.field) {
			op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
			return
		}

		kv.processCmd(op)
	}

}
