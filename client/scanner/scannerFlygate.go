package scanner

import (
	"errors"
	"github.com/sniperHW/flyfish/client"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

type scannerFlygate struct {
	pdAddr []*net.UDPAddr
	conn   net.Conn
	finish bool
	closed bool
}

func (sc *scannerFlygate) close() {
	if !sc.closed {
		sc.closed = true
		if nil != sc.conn {
			sc.conn.Close()
		}
	}
}

func (sc *scannerFlygate) connectFlygate(scanner *Scanner, deadline time.Time) error {
	var gates []*sproto.Flygate
	for len(gates) == 0 && time.Now().Before(deadline) {
		gates = client.QueryGate(sc.pdAddr, deadline.Sub(time.Now()))
	}

	if len(gates) == 0 {
		return errors.New("no available gate")
	}

	if conn, err := scanner.connectServer(gates[0].Service, 0, nil, 0, deadline); nil != err {
		return err
	} else {
		sc.conn = conn
		return nil
	}
	return nil
}

func (sc *scannerFlygate) fetchRows(scanner *Scanner, deadline time.Time) (err error) {

	if sc.closed {
		return ErrClosed
	}

	defer func() {
		if (sc.finish || nil != err) && nil != sc.conn {
			sc.conn.Close()
			sc.conn = nil
		}
	}()

	for !sc.finish && len(scanner.rows) == 0 {
		if nil == sc.conn {
			if err = sc.connectFlygate(scanner, deadline); nil != err {
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
					Fields:  map[string]*client.Field{},
				}
				var err error
				for _, vv := range v.Fields {
					r.Fields[vv.Name], err = client.UnpackField(vv)
					if nil != err {
						client.GetSugar().Infof("scan %s filed:%s unpackField error:%v", v.Key, vv.Name, err)
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
