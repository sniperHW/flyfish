package flyfish

import (
	//"database/sql"
	codec "flyfish/codec"
	"flyfish/conf"
	"flyfish/errcode"
	protocol "flyfish/proto"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet"
	"strings"
	"sync/atomic"
	"time"
)

type scaner struct {
	db             *sqlx.DB
	meta           *table_meta
	session        kendynet.StreamSession
	closed         int32
	offset         int32
	table          string
	fields         []string
	field_receiver []interface{}
	field_convter  []func(interface{}) interface{}
}

func (this *scaner) close() {
	Debugln("scaner close")
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {

		Debugln("scaner close ok")

		if nil != this.db {
			this.db.Close()
		}
	}
}

func scan(session kendynet.StreamSession, msg *codec.Message) {

	u := session.GetUserData()

	req := msg.GetData().(*protocol.ScanReq)

	Debugln("scan")

	var s *scaner

	if u == nil {

		req := msg.GetData().(*protocol.ScanReq)

		resp := &protocol.ScanResp{
			Seqno:   proto.Int64(req.GetSeqno()),
			ErrCode: proto.Int32(errcode.ERR_OK),
		}

		if session.GetUserData() != nil {
			session.Send(resp)
			return
		}

		if "" == req.GetTable() {
			resp.ErrCode = proto.Int32(errcode.ERR_MISSING_TABLE)
			session.Send(resp)
			session.Close("", 1)
			return
		}

		meta := getMetaByTable(req.GetTable())
		if nil == meta {
			resp.ErrCode = proto.Int32(errcode.ERR_INVAILD_TABLE)
			session.Send(resp)
			session.Close("", 1)
			return
		}

		if nil != req.GetFields() {
			for _, name := range req.GetFields() {
				_, ok := meta.fieldMetas[name]
				if !ok {
					resp.ErrCode = proto.Int32(errcode.ERR_INVAILD_FIELD)
					session.Send(resp)
					session.Close("", 1)
					return
				}
			}
		}

		s = &scaner{
			table: req.GetTable(),
		}

		//selectTemplate := "select %s from %s order by __key__;"

		if 1 == req.GetAll() {
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

		s.db, err = pgOpen(conf.PgsqlHost, conf.PgsqlPort, conf.PgsqlDataBase, conf.PgsqlUser, conf.PgsqlPassword)
		if nil != err {
			resp.ErrCode = proto.Int32(errcode.ERR_SQLERROR)
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

const selectTemplate string = "select %s from %s order by __key__ limit %d offset %d;"

func (this *scaner) next(req *protocol.ScanReq) {

	resp := &protocol.ScanResp{
		Seqno:   proto.Int64(req.GetSeqno()),
		ErrCode: proto.Int32(errcode.ERR_SCAN_END),
	}

	if isStop() {
		resp.ErrCode = proto.Int32(errcode.ERR_SERVER_STOPED)
		this.session.Send(resp)
		this.session.Close("", 1)
		return
	}

	deadline := time.Now().Add(time.Duration(req.GetTimeout()))

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
		resp.ErrCode = proto.Int32(errcode.ERR_SQLERROR)
		this.session.Send(resp)
		this.session.Close("", 1)
		return
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(this.field_receiver...)
		if err != nil {
			resp.ErrCode = proto.Int32(errcode.ERR_SQLERROR)
			this.session.Send(resp)
			this.session.Close("scan error", 1)
			Errorln("rows.Scan err", err)
			return
		}

		this.offset++

		if nil == resp.Rows {
			resp.Rows = []*protocol.Row{}
		}

		fields := []*protocol.Field{}
		for i := 0; i < len(this.fields); i++ {
			fields = append(fields, protocol.PackField(this.fields[i], this.field_convter[i](this.field_receiver[i])))
		}

		resp.Rows = append(resp.Rows, &protocol.Row{
			Fields: fields,
		})

		count--

		if 0 == count {
			resp.ErrCode = proto.Int32(errcode.ERR_OK)
			break
		}
	}

	if time.Now().After(deadline) {
		//已经超时
		return
	}

	this.session.Send(resp)

	if resp.GetErrCode() != errcode.ERR_OK {
		this.session.Close("scan finish", 1)
	}
}
