package kvnode

import (
	//"fmt"
	//pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	//"github.com/sniperHW/flyfish/dbmeta"
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet"
	//"time"
)

/*
 * 取消尚未开始执行的客户端请求
 * op准备投入执行前会调用isCancel,如果连接关闭或找不到对应的replyer，
 * isCancel将返回false。op会直接丢弃。
 * cancel操作就是请求的所有seqno对应的replyer删除，使得isCancel返回false
 */

func cancel(n *KVNode, cli *cliConn, msg *codec.Message) {
	req := msg.GetData().(*proto.Cancel)
	for _, v := range req.GetSeqs() {
		cli.removeReplyerBySeqno(v)
	}
}
