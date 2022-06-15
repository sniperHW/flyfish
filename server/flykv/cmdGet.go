package flykv

import (
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdGet struct {
	cmdBase
	wants []string
}

func (this *cmdGet) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Get
}

func (s *kvstore) makeGet(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.GetReq) (cmdI, errcode.Error) {

	if !req.GetAll() {
		if err := kv.meta.CheckFieldsName(req.GetFields()); nil != err {
			return nil, errcode.New(errcode.Errcode_error, err.Error())
		}
	}

	get := &cmdGet{
		cmdBase: cmdBase{
			version: req.Version,
		},
	}

	get.cmdBase.init(get, kv, replyer, seqno, deadline, func(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
		pbdata := &flyproto.GetResp{}

		if err == nil {
			if version > 0 {
				if get.version != nil && *get.version == version {
					err = Err_record_unchange
				} else {
					pbdata.Version = proto.Int64(version)
					for _, name := range get.wants {
						if v := fields[name]; nil != v {
							pbdata.Fields = append(pbdata.Fields, v)
						} else {
							/*
							 * 表格新增加了列，但未设置过，使用默认值
							 */
							pbdata.Fields = append(pbdata.Fields, flyproto.PackField(name, get.meta.GetDefaultValue(name)))
						}
					}
				}
			} else {
				err = Err_record_notexist
			}
		}

		return &cs.RespMessage{
			Seqno: seqno,
			Err:   err,
			Data:  pbdata}
	})

	if req.GetAll() {
		get.wants = kv.meta.GetAllFieldsName()
	} else {
		get.wants = make([]string, 0, len(req.GetFields()))
		for _, k := range req.GetFields() {
			get.wants = append(get.wants, k)
		}
	}

	return get, nil
}
