package flygate

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"sync/atomic"
	"time"
)

type storeScanner struct {
	id       int
	set      int
	slots    *bitmap.Bitmap
	conn     net.Conn
	services map[int]string
}

type scanner struct {
	wantFields   []*flyproto.ScanField
	all          bool
	okSlots      *bitmap.Bitmap
	stores       []*storeScanner
	offset       int
	table        string
	tableVersion int64
}

func (sc *scanner) fillStores(g *gate, slots []int) {
	g.Lock()
	defer g.Unlock()
	if len(slots) == 0 {
		for _, v := range g.sets {
			for _, vv := range v.stores {
				if len(v.nodes) > 0 {
					st := &storeScanner{
						set:      v.setID,
						id:       vv.id,
						slots:    vv.slots.Clone(),
						services: map[int]string{},
					}
					for _, vvv := range v.nodes {
						st.services[vvv.id] = vvv.service
					}
					sc.stores = append(sc.stores, st)
				}
			}
		}
	} else {
		type pairs struct {
			set   int
			slots *bitmap.Bitmap
		}
		stores := map[int]*pairs{}
		for _, v := range slots {
			if store, ok := g.slotToStore[v]; ok {
				p := stores[store.id]
				if nil == p {
					p = &pairs{
						set:   store.setID,
						slots: bitmap.New(sslot.SlotCount),
					}
					stores[store.id] = p
				}
				p.slots.Set(v)
			}
		}

		for _, vv := range stores {
			set := g.sets[vv.set]
			if len(set.nodes) > 0 {
				st := &storeScanner{
					id:       vv.set,
					slots:    vv.slots,
					services: map[int]string{},
				}

				for _, vvv := range set.nodes {
					st.services[vvv.id] = vvv.service
				}
				sc.stores = append(sc.stores, st)
			}
		}
	}
}

func (g *gate) onScanner(conn net.Conn) {
	if atomic.AddInt32(&g.scannerCount, 1) > int32(g.config.MaxScannerCount) {
		atomic.AddInt32(&g.scannerCount, -1)
		conn.Close()
		return
	}

	go func() {
		startScan := false
		defer func() {
			if !startScan {
				atomic.AddInt32(&g.scannerCount, -1)
			}
		}()

		req, err := scan.RecvScannerReq(conn, time.Now().Add(time.Second*5))
		if nil != err {
			GetSugar().Infof("RecvScannerReq error:%v", err)
			conn.Close()
			return
		}

		var sc *scanner

		deadline := time.Now().Add(time.Duration(req.Timeout))

		errCode := func() int {
			if len(req.Fields) == 0 {
				return scan.Err_empty_fields
			}

			//向pd获取table meta
			var resp *sproto.GetScanTableMetaResp
			for nil == resp && !time.Now().After(deadline) {
				if r, err := snet.UdpCall(g.pdAddr, &sproto.GetScanTableMeta{Table: req.Table}, &sproto.GetScanTableMetaResp{}, time.Second); nil == err {
					resp = r.(*sproto.GetScanTableMetaResp)
				}
			}
			if nil == resp {
				return scan.Err_timeout
			} else if resp.TabVersion == -1 {
				return scan.Err_invaild_table
			}

			sc = &scanner{
				okSlots:      bitmap.New(sslot.SlotCount),
				table:        req.Table,
				tableVersion: resp.TabVersion,
			}

			for _, v := range req.Fields {
				version := int64(-1)
				for _, vv := range resp.Fields {
					if v.Field == vv.Field {
						version = vv.Version
						break
					}
				}

				if version >= 0 {
					sc.wantFields = append(sc.wantFields, &flyproto.ScanField{
						Field:   v.Field,
						Version: version,
					})
				} else {
					return scan.Err_invaild_field
				}
			}
			return scan.Err_ok
		}()

		if time.Now().After(deadline) {
			conn.Close()
		} else {
			err = scan.SendScannerResp(conn, errCode, time.Now().Add(time.Second))
			if nil != err {
				GetSugar().Infof("SendScannerResp error:%v", err)
				conn.Close()
			} else if errCode == scan.Err_ok {
				startScan = true
				go sc.loop(g, conn)
			} else {
				conn.Close()
			}
		}
	}()
}

func (st *storeScanner) next(sc *scanner, count int, deadline time.Time) (*flyproto.ScanNextResp, error) {
	if nil == st.conn {
		var leader int
		nodes := []string{}
		for _, v := range st.services {
			nodes = append(nodes, v)
		}

		for 0 == leader && deadline.After(time.Now()) {
			if r, err := snet.UdpCall(nodes, &sproto.QueryLeader{Store: int32(st.id)}, &sproto.QueryLeaderResp{}, time.Second); nil == err {
				leader = int(r.(*sproto.QueryLeaderResp).Leader)
			}
		}

		if 0 == leader {
			return nil, errors.New("no leader")
		}

		dialer := &net.Dialer{Timeout: deadline.Sub(time.Now())}
		conn, err := dialer.Dial("tcp", st.services[leader])
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

		err = scan.SendScannerReq(conn, sc.table, sc.tableVersion, st.slots.ToJson(), st.id, sc.wantFields, deadline)

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
		}

		st.conn = conn
	}

	err := scan.SendScanNextReq(st.conn, count, deadline)
	if nil != err {
		return nil, err
	}

	resp, err := scan.RecvScanNextResp(st.conn, deadline)

	if nil != err {
		return nil, err
	} else {
		return resp, nil
	}
}

func (sc *scanner) loop(g *gate, conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt32(&g.scannerCount, -1)
	}()

	for {
		req, err := scan.RecvScanNextReq(conn, time.Now().Add(scan.RecvScanNextReqTimeout))
		if nil != err {
			return
		}

		deadline := time.Now().Add(time.Duration(req.Timeout))

		for len(sc.stores) == 0 {
			sc.fillStores(g, sc.okSlots.GetCloseBits())
			if time.Now().After(deadline) {
				return
			} else if len(sc.stores) > 0 {
				break
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}

		resp, err := sc.stores[sc.offset].next(sc, int(req.Count), deadline)

		if nil != err {
			GetSugar().Infof("next error:%v", err)
			return
		}

		if scan.Err_ok != int(resp.ErrCode) {
			scan.SendScanNextResp(conn, int(resp.ErrCode), 0, nil, time.Now().Add(time.Second))
			return
		}

		var dummy *flyproto.Row
		if len(resp.Rows) > 0 && resp.Rows[len(resp.Rows)-1].Dummy {
			dummy = resp.Rows[len(resp.Rows)-1]
			//丢弃dummy
			resp.Rows = resp.Rows[:len(resp.Rows)-1]
		}

		if nil != dummy {
			switch scan.GetDummyType(dummy) {
			case scan.DummySlot:
				sc.okSlots.Set(int(resp.Slot))
			case scan.DummyStore:
				sc.stores[sc.offset].conn.Close()
				sc.offset++
				if sc.offset >= len(sc.stores) {
					//store遍历完毕，检查是否有slot因为迁移被遗漏
					if slots := sc.okSlots.GetCloseBits(); len(slots) > 0 {
						sc.stores = sc.stores[:0]
						sc.offset = 0
					} else {
						//加入dummy通告scan结束
						resp.Rows = append(resp.Rows, scan.MakeDummyRow(scan.DummyScan))
					}
				}
			default:
				return
			}
		}

		if nil != scan.SendScanNextResp(conn, 0, 0, resp.Rows, time.Now().Add(time.Second)) {
			return
		} else if len(resp.Rows) > 0 && scan.GetDummyType(resp.Rows[len(resp.Rows)-1]) == scan.DummyScan {
			return
		}
	}
}
