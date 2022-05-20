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

func (g *gate) onScanner(conn net.Conn) {
	if !g.checkReqLimit(int(atomic.AddInt64(&g.totalPendingReq, 1))) {
		conn.Close()
		return
	}

	go func() {
		startScan := false
		defer func() {
			if !startScan {
				atomic.AddInt64(&g.totalPendingReq, -1)
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

			var resp *sproto.GetScanTableMetaResp

			//向pd获取table meta
			for nil == resp && !time.Now().After(deadline) {
				context := snet.MakeUniqueContext()
				if r := snet.UdpCall(g.pdAddr, snet.MakeMessage(context, &sproto.GetScanTableMeta{Table: req.Table}), time.Second, func(respCh chan interface{}, r interface{}) {
					if m, ok := r.(*snet.Message); ok {
						if resp, ok := m.Msg.(*sproto.GetScanTableMetaResp); ok && context == m.Context {
							select {
							case respCh <- resp:
							default:
							}
						}
					}
				}); nil != r {
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

			ch := make(chan struct{})

			g.mainQueue.Append(0, func() {
				for _, v := range g.sets {
					for _, vv := range v.stores {
						st := &storeScanner{
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
				close(ch)
			})

			<-ch

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
			context := snet.MakeUniqueContext()
			if resp := snet.UdpCall(nodes, snet.MakeMessage(context, &sproto.QueryLeader{Store: int32(st.id)}), time.Second, func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.QueryLeaderResp); ok && context == m.Context && 0 != resp.Leader {
						select {
						case respCh <- int(resp.Leader):
						default:
						}
					}
				}
			}); nil != resp {
				leader = resp.(int)
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
		atomic.AddInt64(&g.totalPendingReq, -1)
	}()

	for {
		req, err := scan.RecvScanNextReq(conn, time.Now().Add(scan.RecvScanNextReqTimeout))
		if nil != err {
			return
		}

		deadline := time.Now().Add(time.Duration(req.Timeout))

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

		breakLoop := false

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
						ch := make(chan struct{})
						g.mainQueue.Append(0, func() {
							stores := map[int]*store{}
							for _, v := range slots {
								if s, ok := g.slotToStore[v]; ok {
									stores[v] = s
								}
							}
							for _, vv := range stores {
								set := g.sets[vv.setID]
								st := &storeScanner{
									id:       vv.id,
									slots:    vv.slots.Clone(),
									services: map[int]string{},
								}
								for _, vvv := range set.nodes {
									st.services[vvv.id] = vvv.service
								}
								sc.stores = append(sc.stores, st)
							}
							close(ch)
						})
						<-ch
					} else {
						//加入dummy通告scan结束
						resp.Rows = append(resp.Rows, scan.MakeDummyRow(scan.DummyScan))
						breakLoop = true
					}
				}
			default:
				return
			}
		}

		if nil != scan.SendScanNextResp(conn, 0, 0, resp.Rows, time.Now().Add(time.Second)) || breakLoop {
			return
		}
	}
}
