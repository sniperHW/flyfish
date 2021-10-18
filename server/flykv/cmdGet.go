package flykv

import (
	"time"

	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
)

type cmdGet struct {
	cmdBase
	tbmeta db.TableMeta
	wants  []string
	uniKey string
}

func (this *cmdGet) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	pbdata := &flyproto.GetResp{
		Version: version,
	}

	if err == nil {
		record_not_exist := (version == 0)
		if !record_not_exist {
			if this.version != nil && *this.version == version {
				err = Err_record_unchange

				if this.tbmeta.TableName() == "weapon" {
					GetSugar().Infof("weapon record_unchange:%s %d %d", this.uniKey, len(fields), version)
				}

			} else {
				for _, name := range this.wants {
					v := fields[name]
					if nil != v {
						pbdata.Fields = append(pbdata.Fields, v)
					} else {
						/*
						 * 表格新增加了列，但未设置过，使用默认值
						 */
						vv := this.tbmeta.GetDefaultValue(name)
						if nil != vv {
							pbdata.Fields = append(pbdata.Fields, flyproto.PackField(name, vv))
						}
					}
				}

				if this.tbmeta.TableName() == "weapon" {
					GetSugar().Infof("weapon reply ok:%s %d %d %v", this.uniKey, len(pbdata.Fields), version, this.wants)
				}
			}
		} else {

			if this.tbmeta.TableName() == "weapon" {
				GetSugar().Infof("weapon record_not_exist1:%s %d %d", this.uniKey, len(fields), version)
			}

			err = Err_record_notexist
		}
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data:  pbdata}
}

func (this *cmdGet) onLoadResult(err error, proposal *kvProposal) {
	if err == db.ERR_RecordNotExist {
		if this.tbmeta.TableName() == "weapon" {
			GetSugar().Infof("weapon record_not_exist2:%s", this.uniKey)
		}
	}

	return
}

func (s *kvstore) makeGet(keyvalue *kv, processDeadline time.Time, respDeadline time.Time, c *conn, seqno int64, req *flyproto.GetReq) (cmdI, errcode.Error) {

	if !req.GetAll() {
		if err := keyvalue.tbmeta.CheckFieldsName(req.GetFields()); nil != err {
			return nil, errcode.New(errcode.Errcode_error, err.Error())
		}
	}

	get := &cmdGet{
		tbmeta: keyvalue.tbmeta,
		uniKey: keyvalue.uniKey,
	}

	initCmdBase(&get.cmdBase, flyproto.CmdType_Get, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, get.makeResponse)

	if req.GetAll() {
		get.wants = keyvalue.tbmeta.GetAllFieldsName()
	} else {
		get.wants = make([]string, 0, len(req.GetFields()))
		for _, k := range req.GetFields() {
			get.wants = append(get.wants, k)
		}
	}

	return get, nil
}
