package client

/*
import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"flyfish/errcode"
	"runtime"
	"sync/atomic"
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
func (this *Client) Scaner(table string,fileds string...) *Scaner {
	s := &scaner{
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

func (this *Scaner) Next(count int,callback func(*Scaner,*ResultSet)) {

	if count <= 0 {
		count = 50
	}

	req := &protocol.ScanReq{
		seqno : proto.Int64(atomic.AddInt64(&this.conn.seqno,1)),
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
		seqno    : this.seqno,
		scanCB   : callback,
		scaner   : this,
		req      : this.req,
	}
	this.conn.exec(context)
}

func (this *Scaner) Close() {

}

func (this *Conn) onScanResp(resp *protocol.ScanResp) {
	c := this.removeContext(resp.GetSeqno())
	if nil != c {	
	}	
}
*/


