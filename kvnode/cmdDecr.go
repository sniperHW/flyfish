package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskDecr struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskDecr) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdDecr)

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

		this.errno = errcode.ERR_OK
		oldV := this.fields[cmd.decr.GetName()]
		var newV *proto.Field

		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()-cmd.decr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()-cmd.decr.GetUint())
		}

		this.fields[oldV.GetName()] = newV
		this.version++
	}

	if this.errno != errcode.ERR_OK {
		this.reply()
		this.getKV().store.issueAddkv(this)
	} else {
		this.getKV().store.issueUpdate(this)
	}
}

func newAsynCmdTaskDecr(cmd commandI) *asynCmdTaskDecr {
	return &asynCmdTaskDecr{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdDecr struct {
	*commandBase
	decr *proto.Field
}

func (this *cmdDecr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdDecr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	pbdata := &proto.DecrByResp{
		Version: version,
	}

	if errCode == errcode.ERR_OK {
		pbdata.NewValue = fields[this.decr.GetName()]
	}

	return codec.NewMessage("", codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, pbdata)
}

func (this *cmdDecr) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if status != cache_new && !this.checkVersion(kv.version) {
		this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
		return nil
	}

	task := newAsynCmdTaskDecr(this)

	if status == cache_missing {
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert_update
	} else if status == cache_ok {
		task.fields = map[string]*proto.Field{}
		task.sqlFlag = sql_update
	}

	if status != cache_new {
		oldV := kv.fields[this.decr.GetName()]
		if nil == oldV {
			/*
			 * 表格新增加了列，但未设置过，使用默认值
			 */
			v := kv.meta.GetDefaultV(this.decr.GetName())
			if nil == v {
				this.reply(errcode.ERR_INVAILD_FIELD, nil, kv.version)
				return nil
			} else {
				oldV = proto.PackField(this.decr.GetName(), kv.meta.GetDefaultV(this.decr.GetName()))
			}
		}
		var newV *proto.Field
		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()-this.decr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()-this.decr.GetUint())
		}
		task.fields[oldV.GetName()] = newV
		task.version = kv.version + 1
	}

	return task

}

func decrBy(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdDecr{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		decr: req.GetField(),
	}

	if nil == op.decr {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	var kv *kv

	var err int32

	table, key := head.SplitUniKey()

	if kv, err = n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	if !kv.meta.CheckField(op.decr) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
