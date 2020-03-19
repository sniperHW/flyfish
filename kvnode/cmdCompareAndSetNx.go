package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskCompareAndSetNx struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskCompareAndSetNx) onSqlResp(errno int32) {

	cmd := this.commands[0].(*cmdCompareAndSetNx)

	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.fields = map[string]*proto.Field{}
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		this.sqlFlag = sql_insert_update
		this.errno = errcode.ERR_OK
		this.version = 1
		this.fields[cmd.newV.GetName()] = cmd.newV
		this.getKV().store.issueUpdate(this)
	} else if errno == errcode.ERR_OK {
		if !cmd.checkVersion(this.version) {
			this.errno = errcode.ERR_VERSION_MISMATCH
		} else if !this.fields[cmd.oldV.GetName()].IsEqual(cmd.oldV) {
			this.errno = errcode.ERR_CAS_NOT_EQUAL
		} else {
			this.fields[cmd.oldV.GetName()] = cmd.newV
			this.version++
		}

		if this.errno != errcode.ERR_OK {
			this.reply()
			this.getKV().store.issueAddkv(this)
		} else {
			this.sqlFlag = sql_update
			this.getKV().store.issueUpdate(this)
		}
	}
}

func newAsynCmdTaskCompareAndSetNx(cmd commandI) *asynCmdTaskCompareAndSetNx {
	return &asynCmdTaskCompareAndSetNx{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdCompareAndSetNx struct {
	*commandBase
	oldV *proto.Field
	newV *proto.Field
}

func (this *cmdCompareAndSetNx) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	Debugln("cmdCompareAndSetNx reply", fields)
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdCompareAndSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	pbdata := &proto.CompareAndSetNxResp{
		Version: version,
	}
	//ok时只返回状态不返回字段值
	if errCode != errcode.ERR_OK && nil != fields {
		pbdata.Value = fields[this.oldV.GetName()]
	}

	return codec.NewMessage(codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, pbdata)
}

func (this *cmdCompareAndSetNx) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	if t != nil {
		return t, false
	}

	kv := this.kv

	status := kv.getStatus()

	if status == cache_ok {
		if !this.checkVersion(kv.version) {
			this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
			return t, true
		}

		v := kv.fields[this.oldV.GetName()]

		if nil == v {
			/*
			 * 表格新增加了列，但未设置过，使用默认值
			 */
			v = proto.PackField(this.oldV.GetName(), kv.meta.GetDefaultV(this.oldV.GetName()))
		}

		if !this.oldV.IsEqual(v) {
			this.reply(errcode.ERR_CAS_NOT_EQUAL, nil, kv.version)
			return t, true
		}
	}

	task := newAsynCmdTaskCompareAndSetNx(this)

	if status == cache_ok {
		task.sqlFlag = sql_update
		task.fields = map[string]*proto.Field{}
		task.fields[this.newV.GetName()] = this.newV
		task.version = kv.version + 1
	} else if status == cache_missing {
		task.version = 1
		task.sqlFlag = sql_insert_update
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(this.getKV().meta, &task.fields)
		task.fields[this.newV.GetName()] = this.newV
	}
	return task, true
}

func compareAndSetNx(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdCompareAndSetNx{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		oldV: req.GetOld(),
		newV: req.GetNew(),
	}

	if nil == op.newV || nil == op.oldV {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	table, key := head.SplitUniKey()

	if kv, err := n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	} else {
		op.kv = kv
		if !kv.meta.CheckCompareAndSet(op.newV, op.oldV) {
			op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
			return
		}
		kv.processCmd(op)
	}

}
