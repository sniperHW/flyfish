package flyfish

import(
	"database/sql"
	"github.com/jmoiron/sqlx"
	protocol "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"flyfish/errcode"
	codec "flyfish/codec"
	"sync/atomic"
	"flyfish/conf"
	"fmt"
	"strings"	
)

type scaner struct {
	db        *sqlx.DB
	meta      *table_meta
	rows      *sql.Rows
	session   kendynet.StreamSession
	fields          []string
	field_receiver  []interface{}
	field_convter   []func(interface{})interface{}
	closed    int32
}

func (this *scaner) close() {
	if atomic.CompareAndSwapInt32(&this.closed,0,1) {
		if nil != this.db {
			this.db.Close()
		}

		if nil != this.rows {
			this.rows.Close()
		}
	}
}

func (this *scaner) next(req *protocol.ScanReq) {

	resp := &protocol.ScanResp{
		Seqno : proto.Int64(req.GetSeqno()),
		ErrCode : proto.Int32(errcode.ERR_SCAN_END),
	}

	count := req.GetCount()

	if 0 == count {
		count = 50
	}

	for this.rows.Next() {
		err := this.rows.Scan(this.field_receiver...)
		if err != nil {
			resp.ErrCode = proto.Int32(errcode.ERR_SQLERROR)
			this.session.Send(resp)
			this.session.Close("scan error",1)
			Errorln("rows.Scan err",err)
			return
		}

		if nil == resp.Rows {
			resp.Rows = []*protocol.Row{} 
		}

		fields := []*protocol.Field{}
		for i := 0; i < len(this.fields); i++ {
			fields = append(fields,protocol.PackField(this.fields[i],this.field_convter[i](this.field_receiver[i])))
		}

		resp.Rows = append(resp.Rows,&protocol.Row{
			Fields : fields,
		})

		count--

		if 0 == count {
			resp.ErrCode = proto.Int32(errcode.ERR_OK)
			break
		}
	}

	this.session.Send(resp)

	if resp.GetErrCode() != errcode.ERR_OK {
		this.session.Close("scan finish",1)
	}
}


func scan(session kendynet.StreamSession,msg *codec.Message) {

	u := session.GetUserData()

	req := msg.GetData().(*protocol.ScanReq)

	var s *scaner

	if u == nil {

		req := msg.GetData().(*protocol.ScanReq)

		resp := &protocol.ScanResp{
			Seqno : proto.Int64(req.GetSeqno()),
			ErrCode : proto.Int32(errcode.ERR_OK),
		}

		if session.GetUserData() != nil {
			session.Send(resp)			
			return
		}

		if "" == req.GetTable() {
			resp.ErrCode =  proto.Int32(errcode.ERR_MISSING_TABLE)
			session.Send(resp)
			session.Close("",1)	
			return
		}

		meta := getMetaByTable(req.GetTable())
		if nil == meta {
			resp.ErrCode =  proto.Int32(errcode.ERR_INVAILD_TABLE)
			session.Send(resp)
			session.Close("",1)	
			return
		}


		if nil != req.GetFields() {
			for _,name := range(req.GetFields()) {
				_,ok := meta.fieldMetas[name]
				if !ok {
					resp.ErrCode =  proto.Int32(errcode.ERR_INVAILD_FIELD)
					session.Send(resp)
					session.Close("",1)					
					return
				}
			}
		}


		s := &scaner{}

		selectTemplate := "select %s from %s"

		if 1 == req.GetAll() {
			s.fields = meta.queryMeta.field_names
			s.field_convter = meta.queryMeta.field_convter		
			s.field_receiver = []interface{}{}
			for i := 0; i < len(s.fields); i++ {
				s.field_receiver = append(s.field_receiver,meta.queryMeta.getReceiverByName(s.fields[i]))
			}
		} else {
			s.fields = req.GetFields()
			s.fields = append(s.fields,"__key__")
			s.fields = append(s.fields,"__version__")
			s.field_convter = []func(interface{})interface{}{}
			s.field_receiver = []interface{}{}
			for i := 0; i < len(s.fields); i++ {
				s.field_receiver = append(s.field_receiver,meta.queryMeta.getReceiverByName(s.fields[i]))
				s.field_convter = append(s.field_convter,meta.queryMeta.getConvetorByName(s.fields[i]))
			}
		}

		var err error

		s.db,err = pgOpen(conf.PgsqlHost, conf.PgsqlPort, conf.PgsqlDataBase, conf.PgsqlUser, conf.PgsqlPassword)
		if nil != err {
			resp.ErrCode =  proto.Int32(errcode.ERR_SQLERROR)
			session.Send(resp)
			session.Close("",1)			
			return
		}

		selectStr := fmt.Sprintf(selectTemplate,strings.Join(s.fields,","),req.GetTable())

		s.rows, err = s.db.Query(selectStr)

		if nil != err {
			resp.ErrCode =  proto.Int32(errcode.ERR_SQLERROR)
			session.Send(resp)
			session.Close("",1)			
			return		
		}

		s.session = session
		session.SetUserData(s)
	} else {
		s = u.(*scaner)
	}

	s.next(req)

}
