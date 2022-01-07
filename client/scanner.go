package client

import (
	"errors"
	"fmt"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sync"
	"time"
)

type Row struct {
	Key     string
	Version int64
	Fields  map[string]*Field
}

type Scanner struct {
	sync.Mutex
	pdAddr      []*net.UDPAddr
	soloService string
	slot        int
	conn        net.Conn
	table       string
	fields      []string
	all         bool
	finish      bool
}

func MakeScanner(conf ClientConf, Table string, fields []string, all ...bool) (*Scanner, error) {
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
		sc.soloService = conf.SoloService
	}

	sc.fields = fields
	if len(all) > 0 {
		sc.all = all[0]
	}

	return sc, nil

}

func recvMakeScannerResp(conn net.Conn, deadline time.Time) (*flyproto.MakeScannerResp, error) {
	resp := &flyproto.MakeScannerResp{}
	err := cs.Recv(conn, 4096, resp, deadline)
	return resp, err
}

func recvScanResp(conn net.Conn, deadline time.Time) (*flyproto.ScanResp, error) {
	resp := &flyproto.ScanResp{}
	err := cs.Recv(conn, 4096*1024, resp, deadline)
	return resp, err
}

var ErrScanFinish error = errors.New("scan finish")

func (sc *Scanner) Next(count int, deadline time.Time) ([]*Row, error) {
	sc.Lock()
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