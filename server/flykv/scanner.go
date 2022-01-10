package flykv

import (
	"errors"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"net"
	"time"
)

func SendScanerResp(conn net.Conn, resp *flyproto.MakeScannerResp) error {
	return cs.Send(conn, resp, time.Now().Add(time.Second*5))
}

func RecvScanerReq(conn net.Conn) (*flyproto.MakeScanerReq, error) {
	req := &flyproto.MakeScanerReq{}
	err := cs.Recv(conn, 65535, req, time.Now().Add(time.Second*5))
	return req, err
}

type cacheKv struct {
	key     string
	version int64
	fields  []*flyproto.Field //字段
}

type slot struct {
	slot    int
	kv      []*cacheKv
	offset  int
	exclude []string
}

type scanner struct {
	wantFields []string
	tbmeta     db.TableMeta
	table      string
	slots      []*slot
	offset     int
	scanner    *sql.Scanner
}

func (this *kvnode) onScanner(conn net.Conn) {
	go func() {
		req, err := RecvScanerReq(conn)
		if nil != err {
			conn.Close()
			return
		}

		var store *kvstore
		var ok bool
		var tbmeta db.TableMeta

		this.muS.RLock()
		store, ok = this.stores[int(req.Store)]
		this.muS.RUnlock()

		var wantSlots *bitmap.Bitmap

		err = func() error {
			if !ok {
				return errors.New("store not found")
			} else if !store.isLeader() {
				return errors.New("store is not leader")
			}

			tbmeta = store.meta.GetTableMeta(req.Table)
			if nil == tbmeta {
				return errors.New("store not found")
			}

			wantSlots, err = bitmap.CreateFromJson(req.Slots)

			return err
		}()

		if nil != err {
			SendScanerResp(conn, &flyproto.MakeScannerResp{
				Ok:     false,
				Reason: err.Error(),
			})
			conn.Close()
			return
		}

		var fields []string

		if req.All {
			fields = tbmeta.GetAllFieldsName()
		} else {
			fields = req.Fields
		}

		scanner := &scanner{
			wantFields: fields,
			tbmeta:     tbmeta,
			table:      req.Table,
		}

		ch := make(chan struct{})

		store.mainQueue.q.Append(0, func() {
			for _, v := range wantSlots.GetOpenBits() {
				if store.slots.Test(v) {
					scanner.makeCachekvs(req.Table, v, store.slotsKvMap[v])
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
			go scanner.loop(this, conn)
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

func (sc *scanner) loop(kvnode *kvnode, conn net.Conn) {
	defer func() {
		conn.Close()
		if nil != sc.scanner {
			sc.scanner.Close()
		}
	}()
	for {
		req, err := sc.recv(conn)
		if nil != err {
			return
		}

		rows, slot, finish, err := sc.next(kvnode, int(req.Count))
		resp := &flyproto.ScanResp{}
		if nil == err {
			resp.Slot = int32(slot)
			resp.Rows = rows
			resp.Finish = finish
		} else {
			resp.Error = err.Error()
		}

		errResp := sc.response(conn, resp)

		if nil != errResp || nil != err || (slot == -1 && finish) {
			return
		}
	}
}

func (sc *scanner) next(kvnode *kvnode, count int) (rows []*flyproto.Row, slot int, finish bool, err error) {
	if sc.offset >= len(sc.slots) {
		slot = -1
		finish = true
		return
	}

	if 0 >= count {
		count = 200
	} else if count > 200 {
		count = 200
	}

	s := sc.slots[sc.offset]
	slot = s.slot

	if nil == sc.scanner {
		sc.scanner, err = sql.NewScanner(sc.tbmeta, kvnode.dbc, s.slot, sc.table, sc.wantFields, s.exclude)
		if nil != err {
			return
		}
	}

	var r []*sql.ScannerRow

	if r, err = sc.scanner.Next(count); nil != err {
		return
	} else {
		for _, v := range r {
			rows = append(rows, &flyproto.Row{
				Key:     v.Key,
				Version: v.Version,
				Fields:  sc.fillDefault(v.Fields),
			})
		}
	}

	count -= len(r)

	for count > 0 && s.offset < len(s.kv) {
		rows = append(rows, &flyproto.Row{
			Key:     s.kv[s.offset].key,
			Version: s.kv[s.offset].version,
			Fields:  sc.fillDefault(s.kv[s.offset].fields),
		})
		count--
		s.offset++
	}

	if len(r) == 0 && s.offset >= len(s.kv) {
		finish = true
		sc.offset++
		sc.scanner.Close()
		sc.scanner = nil
	}

	return
}

func (sc *scanner) makeCachekvs(table string, slotNo int, kvs map[string]*kv) {

	s := &slot{
		slot: slotNo,
	}

	if nil != kvs {
		for _, v := range kvs {
			if v.state == kv_ok && v.table == table {
				ckv := &cacheKv{
					key:     v.key,
					version: v.version,
				}

				for _, k := range sc.wantFields {
					if f, ok := v.fields[k]; ok {
						ckv.fields = append(ckv.fields, f)
					}
				}

				s.kv = append(s.kv, ckv)
				s.exclude = append(s.exclude, v.key)
			}
		}
	}

	sc.slots = append(sc.slots, s)

}

func (sc *scanner) fillDefault(fields []*flyproto.Field) []*flyproto.Field {
	for _, name := range sc.wantFields {
		found := false

		for _, v := range fields {
			if v.Name == name {
				found = true
				break
			}
		}

		//field没有被设置过，使用默认值
		if !found {
			if vv := sc.tbmeta.GetDefaultValue(name); nil != vv {
				fields = append(fields, flyproto.PackField(name, vv))
			}
		}
	}

	return fields
}
