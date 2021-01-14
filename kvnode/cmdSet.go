package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskSet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskSet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdSet)

	if !cmd.checkVersion(this.version) {
		this.errno = errcode.ERR_VERSION_MISMATCH
	} else {
		if errno == errcode.ERR_RECORD_NOTEXIST {
			this.fields = map[string]*proto.Field{}
			fillDefaultValue(cmd.getKV().meta, &this.fields)
			this.sqlFlag = sql_insert_update
		} else {
			this.sqlFlag = sql_update
		}

		for k, v := range cmd.fields {
			this.fields[k] = v
		}

		this.errno = errcode.ERR_OK
		this.version++
	}

	if this.errno != errcode.ERR_OK {
		this.reply()
		this.getKV().store.issueAddkv(this)
	} else {
		this.getKV().store.issueUpdate(this)
	}
}

func newAsynCmdTaskSet() *asynCmdTaskSet {
	return &asynCmdTaskSet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{},
		},
	}
}

type cmdSet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	//logger.Debugln("cmdSet reply")
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *net.Message {
	return net.NewMessage(net.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, &proto.SetResp{
		Version: version,
	})

}

func (this *cmdSet) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	task, ok := t.(*asynCmdTaskSet)

	if t != nil && !ok {
		//不同类命令不能合并
		return t, false
	}

	if t != nil && this.version != nil {
		//同类命令但需要检查版本号，不能跟之前的命令合并
		return t, false
	}

	kv := this.kv
	status := kv.getStatus()

	if t != nil && status == cache_new {
		//需要从数据库加载，不合并
		return t, false
	}

	if status != cache_new && !this.checkVersion(kv.version) {
		this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
		return t, true
	}

	if nil == t {
		task = newAsynCmdTaskSet()
		task.fields = map[string]*proto.Field{}

		if status == cache_missing {
			fillDefaultValue(kv.meta, &task.fields)
			task.sqlFlag = sql_insert_update
		} else if status == cache_ok {
			task.sqlFlag = sql_update
		}
	}

	task.commands = append(task.commands, this)

	for k, v := range this.fields {
		task.fields[k] = v
	}

	if status != cache_new {
		if t == nil {
			task.version = kv.version + 1
		}
		//logger.Debugln("task version", task.version)
	}

	return task, true
}

func set(n *KVNode, cli *cliConn, msg *net.Message) {

	req := msg.GetData().(*proto.SetReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdSet{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		fields: map[string]*proto.Field{},
	}

	if len(req.GetFields()) == 0 {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	table, key := head.SplitUniKey()

	if kv, err := n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	} else {

		if !kv.meta.CheckSet(req.GetFields()) {
			op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
			return
		}

		op.kv = kv

		for _, v := range req.GetFields() {
			op.fields[v.GetName()] = v
		}

		kv.processCmd(op)
	}

}
