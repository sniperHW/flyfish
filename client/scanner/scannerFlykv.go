package scanner

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"time"
)

type storeScanner struct {
	id       int
	set      int
	slots    *bitmap.Bitmap
	conn     net.Conn
	services map[int]string
}

type scannerFlykv struct {
	all          bool
	okSlots      *bitmap.Bitmap
	stores       []*storeScanner
	offset       int
	tableVersion int64
	pdAddr       []*net.UDPAddr
	closed       bool
	finish       bool
}

func (sc *scannerFlykv) close() {
	if !sc.closed {
		sc.closed = true
		if len(sc.stores) > sc.offset && sc.stores[sc.offset].conn != nil {
			sc.stores[sc.offset].conn.Close()
		}
	}
}

func (sc *scannerFlykv) fillStoreScanner(resp *sproto.QueryRouteInfoResp) {
	if len(sc.okSlots.GetOpenBits()) == 0 {
		for _, set := range resp.Sets {
			services := map[int]string{}
			for _, kvnode := range set.Kvnodes {
				services[int(kvnode.NodeID)] = fmt.Sprintf("%s:%d", kvnode.Host, kvnode.ServicePort)
			}

			if len(services) > 0 {
				for k, store := range set.Stores {
					st := &storeScanner{
						set:      int(set.SetID),
						id:       int(store),
						services: services,
					}
					st.slots, _ = bitmap.CreateFromJson(set.Slots[k])
					sc.stores = append(sc.stores, st)
				}
			}
		}
	} else {
		type pairs struct {
			set      int
			slots    *bitmap.Bitmap
			services map[int]string
		}
		stores := map[int]*pairs{}
		for _, set := range resp.Sets {
			services := map[int]string{}
			for _, kvnode := range set.Kvnodes {
				services[int(kvnode.NodeID)] = fmt.Sprintf("%s:%d", kvnode.Host, kvnode.ServicePort)
			}

			if len(services) > 0 {
				for k, store := range set.Stores {
					slots, _ := bitmap.CreateFromJson(set.Slots[k])
					for _, v := range sc.okSlots.GetCloseBits() {
						if slots.Test(v) {
							p := stores[int(store)]
							if nil == p {
								p = &pairs{
									set:      int(set.SetID),
									slots:    bitmap.New(sslot.SlotCount),
									services: services,
								}
								stores[int(store)] = p
							}
							p.slots.Set(v)
						}
					}
				}
			}
		}

		for k, v := range stores {
			sc.stores = append(sc.stores, &storeScanner{
				set:      v.set,
				id:       k,
				services: v.services,
				slots:    v.slots,
			})
		}
	}
}

func (st *storeScanner) next(sc *scannerFlykv, table string, fields []*flyproto.ScanField, count int, deadline time.Time) (*flyproto.ScanNextResp, error) {
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

		err = scan.SendScannerReq(conn, table, sc.tableVersion, st.slots.ToJson(), st.id, fields, deadline)

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
		st.conn.Close()
		st.conn = nil
		return nil, err
	} else {
		return resp, nil
	}
}

func (sc *scannerFlykv) fetchRows(scanner *Scanner, deadline time.Time) error {
	if sc.closed {
		return ErrClosed
	}

	if sc.finish {
		return nil
	}

	for {

		if len(sc.stores) == 0 {
			if nil == sc.okSlots {
				sc.okSlots = bitmap.New(sslot.SlotCount)
			}

			if sc.tableVersion == 0 {
				//向pd获取table meta
				var resp *sproto.GetScanTableMetaResp
				for nil == resp && !time.Now().After(deadline) {
					if r, err := snet.UdpCall(sc.pdAddr, &sproto.GetScanTableMeta{Table: scanner.table}, &sproto.GetScanTableMetaResp{}, time.Second); nil == err {
						resp = r.(*sproto.GetScanTableMetaResp)
					}
				}
				if nil == resp {
					return ErrTimeout
				} else if resp.TabVersion == -1 {
					return ErrInvaildTable
				}

				sc.tableVersion = resp.TabVersion

				for _, v := range scanner.fields {
					i := 0
					for ; i < len(resp.Fields); i++ {
						vv := resp.Fields[i]
						if v.Field == vv.Field {
							v.Version = vv.Version
							break
						}
					}

					if i >= len(resp.Fields) {
						return ErrInvaildField
					}
				}
			}

			//获取路由信息
			for {
				resp := client.QueryRouteInfo(sc.pdAddr, &sproto.QueryRouteInfo{})

				if time.Now().After(deadline) {
					return ErrTimeout
				} else if nil != resp {
					sc.fillStoreScanner(resp)
					if len(sc.stores) > 0 {
						break
					}
				}
				time.Sleep(time.Millisecond * 100)
			}
		}

		resp, err := sc.stores[sc.offset].next(sc, scanner.table, scanner.fields, scanner.fetchRowCount, deadline)

		if nil != err {
			client.GetSugar().Infof("next error:%v", err)
			return err
		}

		if scan.Err_ok != int(resp.ErrCode) {
			return fmt.Errorf("scan error errcode:%d", resp.ErrCode)
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
				sc.stores[sc.offset].conn = nil
				sc.offset++
				if sc.offset >= len(sc.stores) {
					//store遍历完毕，检查是否有slot因为迁移被遗漏
					if slots := sc.okSlots.GetCloseBits(); len(slots) > 0 {
						sc.stores = sc.stores[:0]
						sc.offset = 0
					} else {
						sc.finish = true
					}
				}
			default:
				return errors.New("invaild response")
			}
		}

		if time.Now().After(deadline) {
			return ErrTimeout
		} else if len(resp.Rows) > 0 || sc.finish {
			for _, v := range resp.Rows {
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
			}
			return nil
		}
	}
}
