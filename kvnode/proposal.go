package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

func checkAsynCmdTask(task asynCmdTaskI) bool {
	kv := task.getKV()
	kv.Lock()
	defer kv.Unlock()
	sqlFlag := sql_none
	taskSqlFlag := task.getSqlFlag()

	switch kv.getSqlFlag() {
	case sql_none:
		sqlFlag = taskSqlFlag
	case sql_insert:
		if taskSqlFlag == sql_update {
			sqlFlag = sql_insert
		} else if taskSqlFlag == sql_delete {
			sqlFlag = sql_delete
		} else {
			return false
		}
	case sql_delete:
		if taskSqlFlag == sql_insert {
			sqlFlag == taskSqlFlag
		} else {
			return false
		}
	case write_back_update:
		if taskSqlFlag == sql_update || taskSqlFlag == sql_delete {
			sqlFlag = taskSqlFlag
		} else {
			return false
		}
	default:
		return false
	}

	switch sqlFlag {
	case sql_delete, sql_insert:
		task.setProposalType(proposal_snapshot)
	case sql_update:
		task.setProposalType(proposal_update)
	}

	return true
}

//发起更新请求
func (this *kvstore) issueUpdate(task asynCmdTaskI) {

	if task.getSqlFlag() == sql_none {
		panic("sqlflag == sql_none")
	}

	Debugln("issueUpdate")

	if !checkAsynCmdTask(task) {
		task.onError(errcode.ERR_OTHER)
		return
	}

	if err := this.proposeC.AddNoWait(task); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}

//请求向所有副本中新增kv
func (this *kvstore) issueAddkv(task asynCmdTaskI) {
	task.setProposalType(proposal_snapshot)
	if err := this.proposeC.AddNoWait(task); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}
