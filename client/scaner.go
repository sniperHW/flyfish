package client

import (
	"github.com/sniperHW/kendynet"
	"runtime"
	"github.com/sniperHW/kendynet/event"
	"flyfish/errcode"
	protocol "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"sync/atomic"
	//"fmt"
)

type Scaner struct {
	conn 		   *Conn
	callbackQueue  *event.EventQueue       //响应回调的事件队列
	table           string
	fileds          []string
	getAll          bool                   //获取所有字段
	first           int32                      
}

//如果不传fields表示getAll
func (this *Client) Scaner(table string,fileds ...string) *Scaner {
	s := &Scaner{
		callbackQueue : this.callbackQueue,
		table : table,
		fileds : fileds,
	}
	if len(fileds) == 0 {
		s.getAll = true
	}
	s.conn = openConn(this.services[0],this.callbackQueue)
	return s
}

func (this *Scaner) wrapCb(cb func(*Scaner,*MutiResult)) func(*MutiResult) {
	return func(r *MutiResult) {
		defer func(){
			if r := recover(); r != nil {
				buf := make([]byte, 65535)
				l := runtime.Stack(buf, false)
				kendynet.Errorf("%v: %s\n", r, buf[:l])
			}			
		}()		
		cb(this,r)
	}
}

func (this *Scaner) Next(count int32,cb func(*Scaner,*MutiResult)) {

	if count <= 0 {
		count = 50
	}

	req := &protocol.ScanReq{
		Seqno : proto.Int64(atomic.AddInt64(&this.conn.seqno,1)),
	}
	if atomic.CompareAndSwapInt32(&this.first,0,1) {
		req.Table = proto.String(this.table)
		if this.getAll {
			req.All = proto.Int32(1)
		} else {
			req.All = proto.Int32(0)
			req.Fields = []string{}
			for _,v := range(this.fileds) {
				if v != "__key__" || v != "__version__" {
					req.Fields = append(req.Fields,v)
				}
			}
			if len(req.Fields) == 0 {
				req.All = proto.Int32(1)
			}
		}
	}
	req.Count = proto.Int32(count)

	context := &cmdContext {
		seqno : req.GetSeqno(),
		cb : callback{
				tt : cb_muti,
				cb : this.wrapCb(cb),
			},
		req : req,
	}
	this.conn.exec(context)
}

func (this *Scaner) Close() {
	this.conn.Close()
}

func (this *Conn) onScanResp(resp *protocol.ScanResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {	
		ret := MutiResult{
			ErrCode : resp.GetErrCode(),
			Rows    : []*Row{},
		}

		if ret.ErrCode == errcode.ERR_OK {
			for _,v := range(resp.GetRows()){
				fields := v.GetFields()
				r := &Row {
					Fields : map[string]*Field{},
				}

				for _,field := range(fields) {
					if field.GetName() == "__key__" {
						r.Key = field.GetString()
					} else if field.GetName() == "__version__" {
						r.Version = field.GetInt()
					} else {
						r.Fields[field.GetName()] = (*Field)(field)
					}
				}

				ret.Rows = append(ret.Rows,r)
			}
		}
		this.doCallBack(c.cb,&ret)
	}	
}



