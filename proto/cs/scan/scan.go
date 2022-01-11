package scan

import (
	"errors"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"net"
	"time"
)

const (
	Err_ok = int(iota)
	Err_invaild_store
	Err_invaild_table
	Err_invaild_field
	Err_store_not_ready
	Err_unpack
	Err_db
	Err_end
)

var err []error = []error{
	errors.New("no error"),
	errors.New("invaild store"),
	errors.New("invaild table"),
	errors.New("invaild field"),
	errors.New("store not ready for scan"),
	errors.New("error on unpack req"),
	errors.New("db error"),
}

type SentinelType int

const (
	SentinelNone  = 0
	SentinelSlot  = 1 //slot已经scan完毕
	SentinelStore = 2 //store已经scan完毕
)

func ToError(errcode int) error {
	if errcode == Err_ok {
		return nil
	} else if errcode > Err_ok && errcode < Err_end {
		return err[errcode]
	} else {
		return errors.New("unknown error")
	}
}

func MakeSentinelRow(tt SentinelType) *flyproto.Row {
	row := &flyproto.Row{Sentinel: true}
	switch tt {
	case SentinelSlot:
		row.Key = "SentinelSlot"
	case SentinelStore:
		row.Key = "SentinelStore"
	default:
		row.Key = "SentinelNone"
	}
	return row
}

func GetSentinelType(row *flyproto.Row) SentinelType {
	switch row.Key {
	case "SentinelSlot":
		return SentinelSlot
	case "SentinelStore":
		return SentinelStore
	default:
		return SentinelNone
	}
}

func SendScannerReq(conn net.Conn, table string, slots []byte, store int, fields []string, allfields bool, deadline time.Time) error {
	return cs.Send(conn, &flyproto.ScannerReq{
		Table:  table,
		Slots:  slots,
		Store:  int32(store),
		Fields: fields,
		All:    allfields,
	}, deadline)
}

func SendScannerResp(conn net.Conn, errCode int, deadline time.Time) error {
	return cs.Send(conn, &flyproto.ScannerResp{ErrCode: int32(errCode)}, deadline)
}

func SendScanNextResp(conn net.Conn, errCode int, slot int, rows []*flyproto.Row, deadline time.Time) error {
	return cs.Send(conn, &flyproto.ScanNextResp{
		ErrCode: int32(errCode),
		Slot:    int32(slot),
		Rows:    rows,
	}, deadline)
}

func SendScanNextReq(conn net.Conn, count int, deadline time.Time) error {
	return cs.Send(conn, &flyproto.ScanNextReq{Count: int32(count)}, deadline)
}

func RecvScannerReq(conn net.Conn, deadline time.Time) (*flyproto.ScannerReq, error) {
	req := &flyproto.ScannerReq{}
	err := cs.Recv(conn, 65535, req, deadline)
	return req, err
}

func RecvScannerResp(conn net.Conn, deadline time.Time) (*flyproto.ScannerResp, error) {
	req := &flyproto.ScannerResp{}
	err := cs.Recv(conn, 65535, req, deadline)
	return req, err
}

func RecvScanNextReq(conn net.Conn, deadline time.Time) (*flyproto.ScanNextReq, error) {
	resp := &flyproto.ScanNextReq{}
	err := cs.Recv(conn, 65535, resp, deadline)
	return resp, err
}

func RecvScanNextResp(conn net.Conn, deadline time.Time) (*flyproto.ScanNextResp, error) {
	resp := &flyproto.ScanNextResp{}
	err := cs.Recv(conn, 4096*1024, resp, deadline)
	return resp, err
}
