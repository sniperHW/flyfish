package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdGet struct {
	cmdBase
	kv    *kv
	wants []string
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
			} else {
				meta := this.kv.getTableMeta()
				for _, name := range this.wants {
					v := fields[name]
					if nil != v {
						pbdata.Fields = append(pbdata.Fields, v)
					} else {
						/*
						 * 表格新增加了列，但未设置过，使用默认值
						 */
						vv := meta.GetDefaultValue(name)
						if nil != vv {
							pbdata.Fields = append(pbdata.Fields, flyproto.PackField(name, vv))
						}
					}
				}
			}
		} else {
			err = Err_record_notexist
		}
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data:  pbdata}
}

func (this *cmdGet) onLoadResult(err error, proposal *kvProposal) {
	return
}

func (s *kvstore) makeGet(kv *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.GetReq) (cmdI, errcode.Error) {

	meta := kv.getTableMeta()

	if !req.GetAll() {
		if err := meta.CheckFieldsName(req.GetFields()); nil != err {
			return nil, errcode.New(errcode.Errcode_error, err.Error())
		}
	}

	get := &cmdGet{
		kv: kv,
	}

	get.cmdBase.init(flyproto.CmdType_Get, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, get.makeResponse)

	if req.GetAll() {
		get.wants = meta.GetAllFieldsName()
	} else {
		get.wants = make([]string, 0, len(req.GetFields()))
		for _, k := range req.GetFields() {
			get.wants = append(get.wants, k)
		}
	}

	return get, nil
}
