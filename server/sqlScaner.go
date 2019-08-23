package server

import (
	"database/sql"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"strings"
	"sync/atomic"
	"time"
)

type scaner struct {
	db      *sqlx.DB
	meta    *table_meta
	session kendynet.StreamSession
	closed  int32
	//offset         int32
	table          string
	fields         []string
	field_receiver []interface{}
	field_convter  []func(interface{}) interface{}
	rows           *sql.Rows
}

func (this *scaner) close() {
	Infoln("scaner close")
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {

		Infoln("scaner close ok")

		if nil != this.rows {
			this.rows.Close()
		}

		if nil != this.db {
			this.db.Close()
		}

	}
}

func scan(session kendynet.StreamSession, msg *codec.Message) {

	u := session.GetUserData()

	req := msg.GetData().(*proto.ScanReq)

	Debugln("scan")

	var s *scaner

	if u == nil {

		req := msg.GetData().(*proto.ScanReq)

		head := req.GetHead()

		resp := &proto.ScanResp{
			Head: &proto.RespCommon{
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errcode.ERR_OK),
			},
		}

		if session.GetUserData() != nil {
			session.Send(resp)
			return
		}

		if "" == head.GetTable() {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_MISSING_TABLE)
			session.Send(resp)
			session.Close("", 1)
			return
		}

		meta := getMetaByTable(head.GetTable())
		if nil == meta {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_INVAILD_TABLE)
			session.Send(resp)
			session.Close("", 1)
			return
		}

		if nil != req.GetFields() {
			for _, name := range req.GetFields() {
				_, ok := meta.fieldMetas[name]
				if name == "__key__" || name == "__version__" || !ok {
					resp.Head.ErrCode = pb.Int32(errcode.ERR_INVAILD_FIELD)
					session.Send(resp)
					session.Close("", 1)
					return
				}
			}
		}

		s = &scaner{
			table: head.GetTable(),
		}

		if req.GetAll() {
			s.fields = meta.queryMeta.field_names
			s.field_convter = meta.queryMeta.field_convter
			s.field_receiver = []interface{}{}
			for i := 0; i < len(s.fields); i++ {
				s.field_receiver = append(s.field_receiver, meta.queryMeta.getReceiverByName(s.fields[i]))
			}
		} else {
			s.fields = req.GetFields()
			s.fields = append(s.fields, "__key__")
			s.fields = append(s.fields, "__version__")
			s.field_convter = []func(interface{}) interface{}{}
			s.field_receiver = []interface{}{}
			for i := 0; i < len(s.fields); i++ {
				s.field_receiver = append(s.field_receiver, meta.queryMeta.getReceiverByName(s.fields[i]))
				s.field_convter = append(s.field_convter, meta.queryMeta.getConvetorByName(s.fields[i]))
			}
		}

		var err error

		dbConfig := conf.GetConfig().DBConfig

		s.db, err = sqlOpen(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

		if nil != err {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_SQLERROR)
			session.Send(resp)
			session.Close("", 1)
			return
		}

		s.session = session
		session.SetUserData(s)

	} else {
		s = u.(*scaner)
	}

	s.next(req)

}

const selectTemplate string = "select %s from %s order by __key__;"

func (this *scaner) next(req *proto.ScanReq) {
	head := req.GetHead()

	resp := &proto.ScanResp{
		Head: &proto.RespCommon{
			Seqno:   pb.Int64(head.GetSeqno()),
			ErrCode: pb.Int32(errcode.ERR_OK),
		},
	}

	if isStop() {
		resp.Head.ErrCode = pb.Int32(errcode.ERR_SERVER_STOPED)
		this.session.Send(resp)
		this.session.Close("", 1)
		return
	}

	deadline := time.Now().Add(time.Duration(head.GetRespTimeout()))

	count := req.GetCount()

	if 0 == count {
		count = 50
	}

	if nil == this.rows {
		selectStr := fmt.Sprintf(selectTemplate, strings.Join(this.fields, ","), this.table)

		rows, err := this.db.Query(selectStr)

		if nil != err {
			Errorln(selectStr, err)
			resp.Head.ErrCode = pb.Int32(errcode.ERR_SQLERROR)
			this.session.Send(resp)
			this.session.Close("", 1)
			return
		}

		this.rows = rows

		if time.Now().After(deadline) {
			//已经超时
			return
		}
	}

	for this.rows.Next() {
		err := this.rows.Scan(this.field_receiver...)
		if err != nil {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_SQLERROR)
			this.session.Send(resp)
			this.session.Close("scan error", 1)
			Errorln("rows.Scan err", err)
			return
		}

		//this.offset++

		if nil == resp.Rows {
			resp.Rows = []*proto.Row{}
		}

		var (
			key     string
			version int64
		)

		fields := []*proto.Field{}
		for i := 0; i < len(this.fields); i++ {
			if this.fields[i] == "__key__" {
				key = this.field_convter[i](this.field_receiver[i]).(string)
			} else if this.fields[i] == "__version__" {
				version = this.field_convter[i](this.field_receiver[i]).(int64)
			} else {
				fields = append(fields, proto.PackField(this.fields[i], this.field_convter[i](this.field_receiver[i])))
			}
		}

		resp.Rows = append(resp.Rows, &proto.Row{
			Key:     pb.String(key),
			Version: pb.Int64(version),
			Fields:  fields,
		})

		count--

		if 0 == count {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_OK)
			break
		}
	}

	if time.Now().After(deadline) {
		//已经超时
		return
	}

	this.session.Send(resp)

	if resp.Head.GetErrCode() != errcode.ERR_OK {
		this.session.Close("scan finish", 1)
	}

}

/*
const selectTemplate string = "select %s from %s order by __key__ limit %d offset %d;"

func (this *scaner) next(req *proto.ScanReq) {

	head := req.GetHead()

	resp := &proto.ScanResp{
		Head: &proto.RespCommon{
			Seqno:   pb.Int64(head.GetSeqno()),
			ErrCode: pb.Int32(errcode.ERR_OK),
		},
	}

	if isStop() {
		resp.Head.ErrCode = pb.Int32(errcode.ERR_SERVER_STOPED)
		this.session.Send(resp)
		this.session.Close("", 1)
		return
	}

	deadline := time.Now().Add(time.Duration(head.GetTimeout()))

	count := req.GetCount()

	if 0 == count {
		count = 50
	}

	selectStr := fmt.Sprintf(selectTemplate, strings.Join(this.fields, ","), this.table, count, this.offset)

	rows, err := this.db.Query(selectStr)

	if time.Now().After(deadline) {
		//已经超时
		return
	}

	Debugln("query ok")

	if nil != err {
		Errorln(selectStr, err)
		resp.Head.ErrCode = pb.Int32(errcode.ERR_SQLERROR)
		this.session.Send(resp)
		this.session.Close("", 1)
		return
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(this.field_receiver...)
		if err != nil {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_SQLERROR)
			this.session.Send(resp)
			this.session.Close("scan error", 1)
			Errorln("rows.Scan err", err)
			return
		}

		this.offset++

		if nil == resp.Rows {
			resp.Rows = []*proto.Row{}
		}

		var (
			key     string
			version int64
		)

		fields := []*proto.Field{}
		for i := 0; i < len(this.fields); i++ {
			if this.fields[i] == "__key__" {
				key = this.field_convter[i](this.field_receiver[i]).(string)
			} else if this.fields[i] == "__version__" {
				version = this.field_convter[i](this.field_receiver[i]).(int64)
			} else {
				fields = append(fields, proto.PackField(this.fields[i], this.field_convter[i](this.field_receiver[i])))
			}
		}

		resp.Rows = append(resp.Rows, &proto.Row{
			Key:     pb.String(key),
			Version: pb.Int64(version),
			Fields:  fields,
		})

		count--

		if 0 == count {
			resp.Head.ErrCode = pb.Int32(errcode.ERR_OK)
			break
		}
	}

	if time.Now().After(deadline) {
		//已经超时
		return
	}

	this.session.Send(resp)

	if resp.Head.GetErrCode() != errcode.ERR_OK {
		this.session.Close("scan finish", 1)
	}
}
*/
