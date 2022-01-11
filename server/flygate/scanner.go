package flygate

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"net"
	"time"
)

type storeContext struct {
	id       int
	slots    *bitmap.Bitmap
	conn     net.Conn
	services map[int]string
}

type scanner struct {
	wantFields []string
	all        bool
	okSlots    *bitmap.Bitmap
	stores     []*storeContext
	offset     int
	table      string
}

func (g *gate) onScanner(conn net.Conn) {
	go func() {
		req, err := RecvScanerReq(conn)
		if nil != err {
			conn.Close()
			return
		}

		sc := &scanner{
			wantFields: req.Fields,
			all:        req.All,
			okSlots:    bitmap.New(sslot.SlotCount),
			table:      req.Table,
		}

		ch := make(chan struct{})

		g.mainQueue.Append(0, func() {
			for _, v := range g.routeInfo.sets {
				if !v.removed {
					for _, vv := range v.stores {
						st := &storeContext{
							id:       vv.id,
							slots:    vv.slots.Clone(),
							services: map[int]string{},
						}
						for _, vvv := range v.nodes {
							if !vvv.removed {
								st.services[vvv.id] = vvv.service
							}
						}
						sc.stores = append(sc.stores, st)
					}
				}
			}
			close(ch)
		})

		<-ch

		if err = SendScanerResp(conn, &flyproto.MakeScannerResp{
			Ok: true,
		}); nil != err {
			conn.Close()
		} else {
			go sc.loop(g, conn)
		}
	}()
}

func (sc *scanner) recv(conn net.Conn) (*flyproto.ScanReq, error) {
	req := &flyproto.ScanReq{}
	err := cs.Recv(conn, 8192, req, time.Now().Add(time.Second*30))
	return req, err
}

func (sc *scanner) response(conn net.Conn, resp *flyproto.ScanResp) error {
	return cs.Send(conn, resp, time.Now().Add(time.Second*5))
}

func (st *storeContext) next(sc *scanner, count int) (*flyproto.ScanResp, error) {
	if nil == st.conn {
		var leader int
		nodes := []string{}
		for _, v := range st.services {
			nodes = append(nodes, v)
		}

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

		if 0 == leader {
			return nil, errors.New("no leader")
		}

		dialer := &net.Dialer{Timeout: time.Second * 5}
		conn, err := dialer.Dial("tcp", st.services[leader])
		if err != nil {
			return nil, err
		}

		deadline := time.Now().Add(time.Second * 5)

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
			Slots:  st.slots.ToJson(),
			Store:  int32(st.id),
			Fields: sc.wantFields,
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
			conn.Close()
			return nil, errors.New(makeScanerResp.Reason)
		}

		st.conn = conn
	}

	deadline := time.Now().Add(time.Second * 10)

	err := cs.Send(st.conn, &flyproto.ScanReq{Count: int32(count)}, deadline)
	if nil != err {
		return nil, err
	}

	resp, err := recvScanResp(st.conn, deadline)

	if nil != err {
		return nil, err
	} else if "" != resp.Error {
		return nil, errors.New(resp.Error)
	} else {
		return resp, nil
	}
}

func (sc *scanner) loop(g *gate, conn net.Conn) {
	defer func() {
		conn.Close()
	}()

	for {
		req, err := sc.recv(conn)
		if nil != err {
			return
		}

		resp, err := sc.stores[sc.offset].next(sc, int(req.Count))

		if nil != err {
			return
		}

		if resp.Finish {
			if resp.Slot == -1 {
				sc.stores[sc.offset].conn.Close()
				sc.offset++
			} else {
				sc.okSlots.Set(int(resp.Slot))
			}
		}

		finish := true

		if sc.offset >= len(sc.stores) {
			//store遍历完毕，检查是否有slot因为迁移被遗漏
			if slots := sc.okSlots.GetCloseBits(); len(slots) > 0 {
				sc.stores = sc.stores[:0]
				sc.offset = 0
				ch := make(chan struct{})
				g.mainQueue.Append(0, func() {

					stores := map[int]*store{}

					for _, v := range slots {
						if s, ok := g.routeInfo.slotToStore[v]; ok {
							stores[v] = s
						}

					}

					for _, vv := range stores {
						st := &storeContext{
							id:       vv.id,
							slots:    vv.slots.Clone(),
							services: map[int]string{},
						}
						for _, vvv := range vv.set.nodes {
							if !vvv.removed {
								st.services[vvv.id] = vvv.service
							}
						}
						sc.stores = append(sc.stores, st)
					}

					close(ch)
				})
				<-ch
			}
			finish = false
		}

		resp.Finish = finish

		err = sc.response(conn, resp)

		if nil != err || finish {
			return
		}
	}
}
