package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskGet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskGet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_OK || errno == errcode.ERR_RECORD_NOTEXIST {
		//向副本同步插入操作
		this.getKV().store.issueAddkv(this)
	}
}

func newAsynCmdTaskGet() *asynCmdTaskGet {
	return &asynCmdTaskGet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{},
		},
	}
}

type cmdGet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdGet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	//logger.Debugln("cmdGet reply", errCode, version)
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdGet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *net.Message {
	pbdata := &proto.GetResp{
		Version: version,
	}

	if errcode.ERR_OK == errCode {
		if this.version != nil && *this.version == version {
			errCode = errcode.ERR_RECORD_UNCHANGE
		} else {
			for _, field := range this.fields {
				v := fields[field.GetName()]
				if nil != v {
					pbdata.Fields = append(pbdata.Fields, v)
				} else {
					/*
					 * 表格新增加了列，但未设置过，使用默认值
					 */
					vv := this.kv.meta.GetDefaultV(field.GetName())
					if nil != vv {
						pbdata.Fields = append(pbdata.Fields, proto.PackField(field.GetName(), vv))
					}
				}
			}
		}
	}

	return net.NewMessage(net.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, pbdata)
}

func (this *cmdGet) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	task, ok := t.(*asynCmdTaskGet)

	if t != nil && !ok {
		//不同类命令不能合并
		return t, false
	}

	kv := this.kv

	status := kv.getStatus()

	if status == cache_ok && this.version != nil && *this.version == kv.version {
		//kv跟cmdGet提交请求的版本一致，且没有新加过未设置的列，直接返回ERR_RECORD_UNCHANGE
		this.reply(errcode.ERR_RECORD_UNCHANGE, nil, kv.version)
		return t, true
	}

	if nil == t {
		task = newAsynCmdTaskGet()
		if status == cache_missing || status == cache_ok {
			task.fields = this.kv.fields
			task.version = this.kv.version
			if status == cache_missing {
				task.errno = errcode.ERR_RECORD_NOTEXIST
			}
		}
	}

	task.commands = append(task.commands, this)

	return task, true
}

func get(n *KVNode, cli *cliConn, msg *net.Message) {

	req := msg.GetData().(*proto.GetReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdGet{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		fields: map[string]*proto.Field{},
	}

	table, key := head.SplitUniKey()

	if kv, err := n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	} else {

		op.kv = kv

		if req.GetAll() {
			for _, name := range op.kv.meta.GetQueryMeta().GetFieldNames() {
				if name != "__key__" && name != "__version__" {
					op.fields[name] = proto.PackField(name, nil)
				}
			}
		} else {

			if !kv.meta.CheckGet(req.GetFields()) {
				op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
				return
			}

			for _, name := range req.GetFields() {
				op.fields[name] = proto.PackField(name, nil)
			}
		}

		kv.processCmd(op)
	}

}
