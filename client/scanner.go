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

var FetchRowCount int = 100

type Row struct {
	Key     string
	Version int64
	Fields  map[string]*Field
}

type scannerImpl interface {
	fetchRows(*Scanner, time.Time) error
}

type storeScanner struct {
	id    int
	slots *bitmap.Bitmap
	conn  net.Conn
}

type soloFlyKvScanner struct {
	service string
	offset  int
	stores  []*storeScanner
}

type clusterFlykvScanner struct {
	pdAddr []*net.UDPAddr
	conn   net.Conn
	finish bool
}

type flySqlScanner struct {
	service string
	conn    net.Conn
	finish  bool
}

type Scanner struct {
	scannerImpl   scannerImpl
	table         string
	fields        []*flyproto.ScanField
	rows          []*Row
	fetchRowCount int
}

func NewScanner(conf ClientConf, Table string, fields []string) (*Scanner, error) {
	if !(conf.ClientType == ClientType_FlyKv || conf.ClientType == ClientType_FlySql) {
		return nil, errors.New("invaild ClientType")
	} else if nil == conf.SoloConf && nil == conf.ClusterConf {
		return nil, errors.New("SoloConf and ClusterConf is nil")
	}

	sc := &Scanner{table: Table, fetchRowCount: conf.FetchRowCount}

	if 0 >= sc.fetchRowCount {
		sc.fetchRowCount = 100
	}

	if conf.ClientType == ClientType_FlyKv {
		if nil != conf.ClusterConf {
			var pdAddr []*net.UDPAddr
			for _, v := range conf.ClusterConf.PD {
				if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
					pdAddr = append(pdAddr, addr)
				}
			}

			sc.scannerImpl = &clusterFlykvScanner{
				pdAddr: pdAddr,
			}

		} else {

			scannerImpl := &soloFlyKvScanner{
				service: conf.SoloConf.Service,
			}

			st := slot.MakeStoreBitmap(conf.SoloConf.Stores)
			for i := 0; i < len(conf.SoloConf.Stores); i++ {
				scannerImpl.stores = append(scannerImpl.stores, &storeScanner{
					id:    conf.SoloConf.Stores[i],
					slots: st[i],
				})
			}
			sc.scannerImpl = scannerImpl

		}

	} else {
		if nil != conf.ClusterConf {

		} else {
			sc.scannerImpl = &flySqlScanner{
				service: conf.SoloConf.Service,
			}
		}
	}

	for _, v := range fields {
		sc.fields = append(sc.fields, &flyproto.ScanField{
			Field: v,
		})
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
		return sc.scannerImpl.fetchRows(sc, deadline)
	}
}

func (sc *Scanner) connectServer(service string, slots []byte, storeID int, deadline time.Time) (conn net.Conn, err error) {
	dialer := &net.Dialer{Timeout: deadline.Sub(time.Now())}
	conn, err = dialer.Dial("tcp", service)
	if err != nil {
		return nil, err
	}

	if err = cs.SendLoginReq(conn, &flyproto.LoginReq{Scanner: true}, deadline); nil != err {
		conn.Close()
		return nil, err
	}

	loginResp, err := cs.RecvLoginResp(conn, deadline)
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return nil, fmt.Errorf("login failed")
	}

	err = scan.SendScannerReq(conn, sc.table, 0, slots, storeID, sc.fields, deadline)

	if nil != err {
		conn.Close()
		return nil, err
	}

	scannerResp, err := scan.RecvScannerResp(conn, deadline)
	if nil != err {
		conn.Close()
		return nil, err
	} else if int(scannerResp.ErrCode) != scan.Err_ok {
		conn.Close()
		return nil, scan.ToError(int(scannerResp.ErrCode))
	} else {
		return conn, nil
	}
}

func (sc *soloFlyKvScanner) fetchRows(scanner *Scanner, deadline time.Time) (err error) {
	for len(scanner.rows) == 0 {
		if sc.offset >= len(sc.stores) {
			return
		}

		var rows []*Row
		var finish bool
		st := sc.stores[sc.offset]
		rows, finish, err = st.fetchRows(scanner, sc.service, deadline)
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
		if conn, err := scanner.connectServer(service, st.slots.ToJson(), st.id, deadline); nil != err {
			return nil, false, err
		} else {
			st.conn = conn
		}
	}

	var rows []*Row

	for len(rows) == 0 {

		err := scan.SendScanNextReq(st.conn, scanner.fetchRowCount, deadline)
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

				var err error
				for _, vv := range v.Fields {
					r.Fields[vv.Name], err = unpackField(vv)
					if nil != err {
						GetSugar().Infof("scan %s filed:%s unpackField error:%v", v.Key, vv.Name, err)
					}
				}

				rows = append(rows, r)
			} else if scan.GetDummyType(v) == scan.DummyStore {
				return nil, true, nil
			}
		}
	}

	return rows, false, nil
}

func (sc *clusterFlykvScanner) connectGate(scanner *Scanner, deadline time.Time) error {
	var gates []*sproto.Flygate
	for len(gates) == 0 && time.Now().Before(deadline) {
		gates = QueryGate(sc.pdAddr, deadline.Sub(time.Now()))
	}

	if len(gates) == 0 {
		return errors.New("no available gate")
	}

	if conn, err := scanner.connectServer(gates[0].Service, nil, 0, deadline); nil != err {
		return err
	} else {
		sc.conn = conn
		return nil
	}
}

func (sc *clusterFlykvScanner) fetchRows(scanner *Scanner, deadline time.Time) (err error) {
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

		err = scan.SendScanNextReq(sc.conn, scanner.fetchRowCount, deadline)
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
				var err error
				for _, vv := range v.Fields {
					r.Fields[vv.Name], err = unpackField(vv)
					if nil != err {
						GetSugar().Infof("scan %s filed:%s unpackField error:%v", v.Key, vv.Name, err)
					}
				}
				scanner.rows = append(scanner.rows, r)
			} else {
				sc.finish = true
			}
		}
	}
	return
}

func (sc *flySqlScanner) fetchRows(scanner *Scanner, deadline time.Time) (err error) {
	defer func() {
		if (sc.finish || nil != err) && nil != sc.conn {
			sc.conn.Close()
			sc.conn = nil
		}
	}()

	for !sc.finish && len(scanner.rows) == 0 {
		if nil == sc.conn {
			if sc.conn, err = scanner.connectServer(sc.service, nil, 0, deadline); nil != err {
				return
			}
		}

		err = scan.SendScanNextReq(sc.conn, scanner.fetchRowCount, deadline)
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
				var err error
				for _, vv := range v.Fields {
					r.Fields[vv.Name], err = unpackField(vv)
					if nil != err {
						GetSugar().Infof("scan %s filed:%s unpackField error:%v", v.Key, vv.Name, err)
					}
				}
				scanner.rows = append(scanner.rows, r)
			} else {
				sc.finish = true
			}
		}
	}
	return
}
