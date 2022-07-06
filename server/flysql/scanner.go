package flysql

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"sync/atomic"
	"time"
)

type scanner struct {
	wantFields []string
	tbmeta     db.TableMeta
	slot       int
	scanner    *sql.Scanner
	conn       net.Conn
}

func (this *flysql) onScanner(conn net.Conn) {
	atomic.AddInt64(&this.totalPendingReq, 1)
	go func() {
		startScan := false
		defer func() {
			if !startScan {
				atomic.AddInt64(&this.totalPendingReq, -1)
			}
		}()

		req, err := scan.RecvScannerReq(conn, time.Now().Add(time.Second*5))
		if nil != err {
			conn.Close()
			return
		}

		deadline := time.Now().Add(time.Duration(req.Timeout))

		var tbmeta db.TableMeta
		var fields []string

		errCode := func() int {
			this.muMeta.Lock()
			tbmeta = this.meta.GetTableMeta(req.Table)
			this.muMeta.Unlock()
			for _, v := range req.Fields {
				if tbmeta.CheckFieldWithVersion(v.Field, v.Version) {
					fields = append(fields, v.Field)
				} else {
					//字段不存在，或删除后又添加，已经不符合请求要求的版本
					return scan.Err_invaild_field
				}
			}
			return scan.Err_ok
		}()

		if time.Now().After(deadline) {
			conn.Close()
			return
		}

		if errCode != scan.Err_ok {
			scan.SendScannerResp(conn, errCode, time.Now().Add(time.Second))
			conn.Close()
			return
		}

		scanner := &scanner{
			wantFields: fields,
			tbmeta:     tbmeta,
		}

		if err = scan.SendScannerResp(conn, scan.Err_ok, time.Now().Add(time.Second)); nil != err {
			conn.Close()
		} else {
			startScan = true
			go scanner.loop(this, conn)
		}
	}()
}

func (sc *scanner) loop(flysql *flysql, conn net.Conn) {
	defer func() {
		conn.Close()
		if nil != sc.scanner {
			sc.scanner.Close()
		}
		atomic.AddInt64(&flysql.totalPendingReq, -1)
	}()
	for {
		req, err := scan.RecvScanNextReq(conn, time.Now().Add(scan.RecvScanNextReqTimeout))
		if nil != err {
			return
		}

		if breakloop := sc.next(flysql, conn, int(req.Count), time.Now().Add(time.Duration(req.Timeout))); breakloop {
			return
		}
	}
}

func (sc *scanner) next(flysql *flysql, conn net.Conn, count int, deadline time.Time) (breakloop bool) {
	resp := &flyproto.ScanNextResp{}
	defer func() {
		if time.Now().After(deadline) {
			breakloop = true
		} else if nil != cs.Send(conn, resp, time.Now().Add(time.Second)) {
			breakloop = true
		} else {
			breakloop = breakloop || resp.ErrCode != int32(scan.Err_ok)
		}
	}()

	if sc.slot > sslot.SlotCount {
		//哑元标识遍历结束
		resp.Rows = append(resp.Rows, scan.MakeDummyRow(scan.DummyScan))
		breakloop = true
		return
	}

	if 0 >= count {
		count = 200
	} else if count > 200 {
		count = 200
	}

	var err error

	if nil == sc.scanner {
		sc.scanner, err = sql.NewScanner(sc.tbmeta, flysql.dbc, sc.slot, sc.wantFields, nil)
		if nil != err {
			resp.ErrCode = int32(scan.Err_db)
			return
		}
	}

	var r []*sql.ScannerRow

	if r, err = sc.scanner.Next(count); nil != err {
		resp.ErrCode = int32(scan.Err_db)
		return
	} else {
		for _, v := range r {
			resp.Rows = append(resp.Rows, &flyproto.Row{
				Key:     v.Key,
				Version: v.Version,
				Fields:  sc.fillDefault(v.Fields),
			})
		}
	}

	count -= len(r)

	if len(r) == 0 {
		//当前slot遍历完毕，递增slot
		sc.slot++
		sc.scanner.Close()
		sc.scanner = nil
	}

	return
}

func (sc *scanner) fillDefault(fields []*flyproto.Field) []*flyproto.Field {
	for _, name := range sc.wantFields {
		found := false

		for _, v := range fields {
			if v.Name == name {
				found = true
				break
			}
		}

		//field没有被设置过，使用默认值
		if !found {
			if vv := sc.tbmeta.GetDefaultValue(name); nil != vv {
				fields = append(fields, flyproto.PackField(name, vv))
			}
		}
	}

	return fields
}
