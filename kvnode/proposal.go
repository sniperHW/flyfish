package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
)

type proposal struct {
	tt     int
	values []interface{}
}

func appendProposal2Str(s *str.Str, tt int, values ...interface{}) {
	switch tt {
	case proposal_lease:
		s.AppendByte(byte(tt))
		s.AppendInt32(int32(values[0].(int)))
		s.AppendInt64(int64(values[1].(uint64)))
	case proposal_snapshot, proposal_update, proposal_kick:
		s.AppendByte(byte(tt))
		unikey := values[0].(string)
		s.AppendInt32(int32(len(unikey)))
		s.AppendString(unikey)
		if tt != proposal_kick {
			version := values[1].(int64)
			s.AppendInt64(version)
			//fields数量
			pos := s.Len()
			s.AppendInt32(int32(0))
			if len(values) == 3 && nil != values[2] {
				fields := values[2].(map[string]*proto.Field)
				c := int32(0)
				for n, v := range fields {
					if n != "__version__" {
						c++
						s.AppendField(v)
					}
				}
				if c > 0 {
					s.SetInt32(pos, c)
				}
			}
		}
	}
}

func readProposal(s *str.Str, offset int) (*proposal, int) {
	var err error
	var tt byte
	tt, offset, err = s.ReadByte(offset)
	if nil != err {
		return nil, 0
	}

	p := &proposal{
		tt: int(tt),
	}

	switch int(tt) {
	case proposal_lease:
		var id int32
		id, offset, err = s.ReadInt32(offset)
		if nil != err {
			return nil, 0
		}
		p.values = append(p.values, int(id))

		var i64 int64
		i64, offset, err = s.ReadInt64(offset)
		if nil != err {
			return nil, 0
		}

		p.values = append(p.values, uint64(i64))

		return p, offset
	case proposal_snapshot, proposal_update, proposal_kick:
		var unikeyLen int32
		unikeyLen, offset, err = s.ReadInt32(offset)
		if nil != err {
			return nil, 0
		}
		var unikey string
		unikey, offset, err = s.ReadString(offset, int(unikeyLen))
		if nil != err {
			return nil, 0
		}
		p.values = append(p.values, unikey)
		if int(tt) == proposal_kick {
			return p, offset
		} else {
			var version int64
			version, offset, err = s.ReadInt64(offset)
			if nil != err {
				return nil, 0
			}
			p.values = append(p.values, version)
			var fieldCount int32
			fieldCount, offset, err = s.ReadInt32(offset)
			if nil != err {
				return nil, 0
			}

			if fieldCount > 0 {
				fields := make([]*proto.Field, int(fieldCount))
				for i := 0; i < int(fieldCount); i++ {
					var v *proto.Field
					v, offset, err = s.ReadField(offset)
					if nil != err {
						return nil, 0
					}
					fields[i] = v
				}
				p.values = append(p.values, fields)
			}
			return p, offset
		}
	default:
		return nil, 0
	}

}

func checkAsynCmdTask(task asynCmdTaskI) bool {
	kv := task.getKV()
	kv.Lock()
	defer kv.Unlock()
	sqlFlag := sql_none
	taskSqlFlag := task.getSqlFlag()

	switch kv.getSqlFlag() {
	case sql_none:
		sqlFlag = taskSqlFlag
	case sql_insert_update:
		if taskSqlFlag == sql_update {
			sqlFlag = sql_insert_update
		} else if taskSqlFlag == sql_delete {
			sqlFlag = sql_delete
		} else {
			return false
		}
	case sql_delete:
		if taskSqlFlag == sql_insert_update {
			sqlFlag = taskSqlFlag
		} else {
			return false
		}
	case sql_update:
		if taskSqlFlag == sql_update || taskSqlFlag == sql_delete {
			sqlFlag = taskSqlFlag
		} else {
			return false
		}
	default:
		return false
	}

	switch sqlFlag {
	case sql_delete, sql_insert_update:
		task.setProposalType(proposal_snapshot)
	case sql_update:
		if kv.isSnapshoted() {
			task.setProposalType(proposal_update)
		} else {
			task.setProposalType(proposal_snapshot)
			task.fillMissingFields(kv.fields)
		}
	}

	return true
}

//发起更新请求
func (this *kvstore) issueUpdate(task asynCmdTaskI) {

	if task.getSqlFlag() == sql_none {
		panic("sqlflag == sql_none")
	}

	logger.Debugln("issueUpdate")

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
