package scanner

//not thread safe don't call concurrently
import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/client"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	"net"
	"time"
)

var FetchRowCount int = 100

type Row struct {
	Key     string
	Version int64
	Fields  map[string]*client.Field
}

type scannerImpl interface {
	fetchRows(*Scanner, time.Time) error
	close()
}

type ScannerType int

const (
	FlyKv   = ScannerType(1) //请求发往flykv
	FlySql  = ScannerType(2) //请求发往flysql
	FlyGate = ScannerType(3) //请求发往flygate由flygate负责转发
)

var (
	ErrTimeout      = errors.New("timeout")
	ErrInvaildTable = errors.New("invaild table")
	ErrInvaildField = errors.New("invaild filed")
	ErrClosed       = errors.New("scanner closed")
)

type ScannerConf struct {
	ScannerType   ScannerType
	FetchRowCount int
	PD            []string
}

type Scanner struct {
	scannerImpl   scannerImpl
	table         string
	fields        []*flyproto.ScanField
	rows          []*Row
	fetchRowCount int
}

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

func (sc *Scanner) Close() {
	sc.scannerImpl.close()
}

func (sc *Scanner) fetchRows(deadline time.Time) error {
	if len(sc.rows) > 0 {
		return nil
	} else {
		return sc.scannerImpl.fetchRows(sc, deadline)
	}
}

func (sc *Scanner) connectServer(service string, metaVersion int64, slots []byte, storeID int, deadline time.Time) (conn net.Conn, err error) {
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

	err = scan.SendScannerReq(conn, sc.table, metaVersion, slots, storeID, sc.fields, deadline)

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

func New(conf ScannerConf, Table string, fields []string) (*Scanner, error) {
	sc := &Scanner{table: Table, fetchRowCount: conf.FetchRowCount}

	if 0 >= sc.fetchRowCount {
		sc.fetchRowCount = 100
	}

	var pdAddr []*net.UDPAddr
	for _, v := range conf.PD {
		if addr, err := net.ResolveUDPAddr("udp", v); nil == err {
			pdAddr = append(pdAddr, addr)
		}
	}

	if len(pdAddr) == 0 {
		return nil, errors.New("pd is empty")
	}

	switch conf.ScannerType {
	case FlyGate:
		sc.scannerImpl = &scannerFlygate{
			pdAddr: pdAddr,
		}
	case FlyKv:
		sc.scannerImpl = &scannerFlykv{
			pdAddr: pdAddr,
		}
	}

	/*if conf.ScannerType == FlyGate {
		sc.scannerImpl = &scannerFlygate{
			pdAddr: pdAddr,
		}
	} else {
		/*if nil != conf.ClusterConf {

		} else {
			sc.scannerImpl = &flySqlScanner{
				service: conf.SoloConf.Service,
			}
		}* /
	}*/

	for _, v := range fields {
		sc.fields = append(sc.fields, &flyproto.ScanField{
			Field: v,
		})
	}

	return sc, nil

}
