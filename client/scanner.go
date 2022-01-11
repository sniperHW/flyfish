package client

import (
	"errors"
	"fmt"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	//snet "github.com/sniperHW/flyfish/server/net"
	//sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/proto/cs/scan"
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

type Scanner struct {
	pdAddr      []*net.UDPAddr
	soloScanner *soloScanner
	table       string
	fields      []string
	allfields   bool
	rows        []*Row
}

func NewScanner(conf ClientConf, Table string, fields []string, allfields ...bool) (*Scanner, error) {
	if "" == conf.SoloService && len(conf.PD) == 0 {
		return nil, errors.New("cluster mode,but pd empty")
	}

	sc := &Scanner{table: Table}

	if "" == conf.SoloService {
		for _, v := range conf.PD {
			if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
				sc.pdAddr = append(sc.pdAddr, addr)
			}
		}
		if len(sc.pdAddr) == 0 {
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
			return nil
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
			if !v.Sentinel {
				r := &Row{
					Key:     v.Key,
					Version: v.Version,
					Fields:  map[string]*Field{},
				}

				for _, vv := range v.Fields {
					r.Fields[vv.Name] = (*Field)(vv)
				}

				rows = append(rows, r)
			} else if scan.GetSentinelType(v) == scan.SentinelStore {
				return nil, true, nil
			}
		}
	}

	return rows, false, nil
}

/*
var ErrScanFinish error = errors.New("scan finish")

func (st *storeScanner) next(scaner *Scanner, service string, count int, deadline time.Time) ([]*Row, bool, error) {
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

		err = scan.SendScannerReq(conn, scaner.table, st.slots.ToJson(), st.id, scaner.fields, scaner.allfields, deadline)

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

	err := scan.SendScanNextReq(st.conn, count, deadline)
	if nil != err {
		return nil, false, err
	}

	resp, err := scan.RecvScanNextResp(st.conn, deadline)

	if nil != err {
		return nil, false, err
	} else if int(scannerResp.ErrCode) != scan.Err_ok {
		return nil, false, scan.ToError(int(resp.ErrCode))
	}

	var rows []*Row
	for _, v := range resp.Rows {
		r := &Row{
			Key:     v.Key,
			Version: v.Version,
			Fields:  map[string]*Field{},
		}

		for _, vv := range v.Fields {
			r.Fields[vv.Name] = (*Field)(vv)
		}

		rows = append(rows, r)
	}

	//finish := false

	//if resp.Slot == -1 && resp.Finish {
	//	finish = true
	//}

	//return rows, finish, nil

}

func (sc *soloScanner) next(scaner *Scanner, count int, deadline time.Time) (rows []*Row, finish bool, err error) {
	if sc.offset >= len(sc.stores) {
		finish = true
		return
	}

	st := sc.stores[sc.offset]
	rows, finish, err = st.next(scaner, sc.soloService, count, deadline)
	if finish {
		finish = false
		sc.offset++
		if sc.offset >= len(sc.stores) {
			finish = true
		}
	}

	if nil != err && nil != st.conn {
		st.conn.Close()
	}

	return
}

func (sc *Scanner) nextSolo(count int, deadline time.Time) (rows []*Row, err error) {
	sc.Lock()
	defer sc.Unlock()
	if !sc.finish {
		rows, sc.finish, err = sc.soloScanner.next(sc, count, deadline)
	} else {
		err = ErrScanFinish
	}
	return
}

func (sc *Scanner) Next(count int, deadline time.Time) ([]*Row, error) {
	if nil != sc.soloScanner {
		return sc.nextSolo(count, deadline)
	} else {
		return nil, ErrScanFinish
	}

	/*sc.Lock()
	defer sc.Unlock()
	if sc.finish {
		return nil, ErrScanFinish
	}

	if nil == sc.conn {
		var store int
		var service string

		if sc.soloService == "" {
			for {
				//向pd查询slot所在store
				context := snet.MakeUniqueContext()
				resp := snet.UdpCall(sc.pdAddr, snet.MakeMessage(context, &sproto.GetSlotStore{Slot: int32(sc.slot)}), deadline.Sub(time.Now()), func(respCh chan interface{}, r interface{}) {
					if m, ok := r.(*snet.Message); ok {
						if resp, ok := m.Msg.(*sproto.GetSlotStoreResp); ok && context == m.Context {
							select {
							case respCh <- resp:
							default:
							}
						}
					}
				})

				if nil == resp {
					return nil, errors.New("timeout")
				} else if 0 != resp.(*sproto.GetSlotStoreResp).Store {
					store = int(resp.(*sproto.GetSlotStoreResp).Store)
					service = resp.(*sproto.GetSlotStoreResp).Service
					break
				}
			}
		} else {
			service = sc.soloService
		}

		dialer := &net.Dialer{Timeout: deadline.Sub(time.Now())}
		conn, err := dialer.Dial("tcp", service)
		if err != nil {
			return nil, err
		}

		if !cs.SendLoginReq(conn, &flyproto.LoginReq{Scanner: true}, deadline) {
			conn.Close()
			return nil, fmt.Errorf("login failed")
		}

		loginResp, err := cs.RecvLoginResp(conn, deadline)
		if nil != err || !loginResp.GetOk() {
			conn.Close()
			return nil, fmt.Errorf("login failed")
		}

		err = cs.Send(conn, &flyproto.MakeScanerReq{
			Table:  sc.table,
			Slot:   int32(sc.slot),
			Store:  int32(store),
			Fields: sc.fields,
			All:    sc.all,
		}, deadline)

		if nil != err {
			conn.Close()
			return nil, err
		}

		makeScanerResp, err := recvMakeScannerResp(conn, deadline)
		if nil != err {
			conn.Close()
			return nil, err
		} else if !makeScanerResp.Ok {
			return nil, errors.New(makeScanerResp.Reason)
		}

		sc.conn = conn
	}

	err := cs.Send(sc.conn, &flyproto.ScanReq{Count: int32(count)}, deadline)
	if nil != err {
		return nil, err
	}

	resp, err := recvScanResp(sc.conn, deadline)

	if nil != err {
		return nil, err
	} else if "" != resp.Error {
		return nil, errors.New(resp.Error)
	}

	var rows []*Row
	for _, v := range resp.Rows {
		r := &Row{
			Key:     v.Key,
			Version: v.Version,
			Fields:  map[string]*Field{},
		}

		for _, vv := range v.Fields {
			r.Fields[vv.Name] = (*Field)(vv)
		}

		rows = append(rows, r)
	}

	if resp.Finish {
		sc.conn.Close()
		sc.conn = nil
		sc.slot++
		if sc.slot >= slot.SlotCount {
			sc.finish = true
		}
	}

	return rows, nil
}

*/
