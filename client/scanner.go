package client

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"time"
)

type Row struct {
	Key     string
	Version int64
	Fields  map[string]*Field
}

type storeScanner struct {
	id    int
	slots *bitmap.Bitmap
	conn  net.Conn
}

type soloScanner struct {
	soloService string
	offset      int
	stores      []*storeScanner
}

type clusterScanner struct {
	pdAddr []*net.UDPAddr
	conn   net.Conn
	finish bool
}

type Scanner struct {
	soloScanner    *soloScanner
	clusterScanner *clusterScanner
	table          string
	fields         []string
	allfields      bool
	rows           []*Row
}

func NewScanner(conf ClientConf, Table string, fields []string, allfields ...bool) (*Scanner, error) {
	if "" == conf.SoloService && len(conf.PD) == 0 {
		return nil, errors.New("cluster mode,but pd empty")
	}

	sc := &Scanner{table: Table}

	if "" == conf.SoloService {

		sc.clusterScanner = &clusterScanner{}

		for _, v := range conf.PD {
			if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
				sc.clusterScanner.pdAddr = append(sc.clusterScanner.pdAddr, addr)
			}
		}
		if len(sc.clusterScanner.pdAddr) == 0 {
			return nil, errors.New("pd is empty")
		}
	} else {
		sc.soloScanner = &soloScanner{
			soloService: conf.SoloService,
		}
		st := slot.MakeStoreBitmap(conf.Stores)
		for i := 0; i < len(conf.Stores); i++ {
			sc.soloScanner.stores = append(sc.soloScanner.stores, &storeScanner{
				id:    conf.Stores[i],
				slots: st[i],
			})
		}
	}

	sc.fields = fields
	if len(allfields) > 0 {
		sc.allfields = allfields[0]
	}

	return sc, nil

}

//don't call Next concurrently
func (sc *Scanner) Next(deadline time.Time) (*Row, error) {
	err := sc.fetchRows(deadline)
	if nil != err {
		return nil, err
	}

	if len(sc.rows) > 0 {
		row := sc.rows[0]
		sc.rows = sc.rows[1:]
		return row, nil
	} else {
		return nil, nil
	}
}

func (sc *Scanner) fetchRows(deadline time.Time) error {
	if len(sc.rows) > 0 {
		return nil
	} else {
		if nil != sc.soloScanner {
			return sc.soloScanner.fetchRows(sc, deadline)
		} else {
			return sc.clusterScanner.fetchRows(sc, deadline)
		}
	}
}

func (sc *soloScanner) fetchRows(scanner *Scanner, deadline time.Time) (err error) {
	for len(scanner.rows) == 0 {
		if sc.offset >= len(sc.stores) {
			return
		}

		var rows []*Row
		var finish bool
		st := sc.stores[sc.offset]
		rows, finish, err = st.fetchRows(scanner, sc.soloService, deadline)
		if finish {
			sc.offset++
		} else {
			scanner.rows = rows
		}

		if finish || nil != err && nil != st.conn {
			st.conn.Close()
		}
	}
	return
}

func (st *storeScanner) fetchRows(scanner *Scanner, service string, deadline time.Time) ([]*Row, bool, error) {
	if nil == st.conn {
		dialer := &net.Dialer{Timeout: deadline.Sub(time.Now())}
		conn, err := dialer.Dial("tcp", service)
		if err != nil {
			return nil, false, err
		}

		if !cs.SendLoginReq(conn, &flyproto.LoginReq{Scanner: true}, deadline) {
			conn.Close()
			return nil, false, fmt.Errorf("login failed")
		}

		loginResp, err := cs.RecvLoginResp(conn, deadline)
		if nil != err || !loginResp.GetOk() {
			conn.Close()
			return nil, false, fmt.Errorf("login failed")
		}

		err = scan.SendScannerReq(conn, scanner.table, st.slots.ToJson(), st.id, scanner.fields, scanner.allfields, deadline)

		if nil != err {
			conn.Close()
			return nil, false, err
		}

		scannerResp, err := scan.RecvScannerResp(conn, deadline)
		if nil != err {
			conn.Close()
			return nil, false, err
		} else if int(scannerResp.ErrCode) != scan.Err_ok {
			conn.Close()
			return nil, false, scan.ToError(int(scannerResp.ErrCode))
		}

		st.conn = conn
	}

	var rows []*Row

	for len(rows) == 0 {

		err := scan.SendScanNextReq(st.conn, 100, deadline)
		if nil != err {
			return nil, false, err
		}

		resp, err := scan.RecvScanNextResp(st.conn, deadline)

		if nil != err {
			return nil, false, err
		} else if int(resp.ErrCode) != scan.Err_ok {
			return nil, false, scan.ToError(int(resp.ErrCode))
		}

		for _, v := range resp.Rows {
			if !v.Dummy {
				r := &Row{
					Key:     v.Key,
					Version: v.Version,
					Fields:  map[string]*Field{},
				}

				for _, vv := range v.Fields {
					r.Fields[vv.Name] = (*Field)(vv)
				}

				rows = append(rows, r)
			} else if scan.GetDummyType(v) == scan.DummyStore {
				return nil, true, nil
			}
		}
	}

	return rows, false, nil
}

func (sc *clusterScanner) connectGate(scanner *Scanner, deadline time.Time) error {
	var gates []*sproto.Flygate
	for time.Now().Before(deadline) {
		gates = QueryGate(sc.pdAddr, deadline.Sub(time.Now()))
	}
	if len(gates) == 0 {
		return errors.New("no available gate")
	}

	dialer := &net.Dialer{Timeout: deadline.Sub(time.Now())}
	conn, err := dialer.Dial("tcp", gates[0].Service)
	if err != nil {
		return err
	}

	if !cs.SendLoginReq(conn, &flyproto.LoginReq{Scanner: true}, deadline) {
		conn.Close()
		return fmt.Errorf("login failed")
	}

	loginResp, err := cs.RecvLoginResp(conn, deadline)
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return fmt.Errorf("login failed")
	}

	err = scan.SendScannerReq(conn, scanner.table, nil, 0, scanner.fields, scanner.allfields, deadline)

	if nil != err {
		conn.Close()
		return err
	}

	scannerResp, err := scan.RecvScannerResp(conn, deadline)
	if nil != err {
		conn.Close()
		return err
	} else if int(scannerResp.ErrCode) != scan.Err_ok {
		conn.Close()
		return scan.ToError(int(scannerResp.ErrCode))
	}

	sc.conn = conn

	return nil
}

func (sc *clusterScanner) fetchRows(scanner *Scanner, deadline time.Time) (err error) {
	defer func() {
		if (sc.finish || nil != err) && nil != sc.conn {
			sc.conn.Close()
			sc.conn = nil
		}
	}()

	for !sc.finish && len(scanner.rows) == 0 {
		if nil == sc.conn {
			if err = sc.connectGate(scanner, deadline); nil != err {
				return
			}
		}

		err = scan.SendScanNextReq(sc.conn, 100, deadline)
		if nil != err {
			return
		}

		var resp *flyproto.ScanNextResp

		resp, err = scan.RecvScanNextResp(sc.conn, deadline)

		if nil != err {
			return
		} else if int(resp.ErrCode) != scan.Err_ok {
			err = scan.ToError(int(resp.ErrCode))
			return
		}

		for _, v := range resp.Rows {
			if !v.Dummy {
				r := &Row{
					Key:     v.Key,
					Version: v.Version,
					Fields:  map[string]*Field{},
				}
				for _, vv := range v.Fields {
					r.Fields[vv.Name] = (*Field)(vv)
				}
				scanner.rows = append(scanner.rows, r)
			} else {
				sc.finish = true
			}
		}
	}
	return
}
