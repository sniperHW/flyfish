package flykv

import (
	"errors"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
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
	err := cs.Recv(conn, 1024, req, time.Now().Add(time.Second*5))
	return req, err
}

type cacheKv struct {
	key     string
	version int64
	fields  []*flyproto.Field //字段
}

type scanner struct {
	wantFields []string
	kv         []*cacheKv
	offset     int
	tbmeta     db.TableMeta
	exclude    []string
	slot       int
	table      string
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

		if req.Store > 0 {
			store, ok = this.stores[int(req.Store)]
		} else {
			//solo mode,slot should not transfer
			for _, v := range this.stores {
				if v.slots.Test(int(req.Slot)) {
					store = v
					ok = true
					break
				}
			}
		}

		this.muS.RUnlock()

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

			return nil
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
			slot:       int(req.Slot),
			table:      req.Table,
		}

		ch := make(chan struct{})

		store.mainQueue.q.Append(0, func() {
			kvs := store.slotsKvMap[int(req.Slot)]
			if nil != kvs {
				scanner.makeCachekvs(req.Table, kvs)
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
	scanner, err := sql.NewScanner(sc.tbmeta, kvnode.dbc, sc.slot, sc.table, sc.wantFields, sc.exclude)

	if nil != err {
		sc.response(conn, &flyproto.ScanResp{Error: err.Error()})
		conn.Close()
		return
	}

	defer func() {
		scanner.Close()
		conn.Close()
	}()
	for {
		req, err := sc.recv(conn)
		if nil != err {
			return
		}
		rows, finish, err := sc.next(scanner, int(req.Count))
		resp := &flyproto.ScanResp{}
		if nil == err {
			resp.Rows = rows
			resp.Finish = finish
		} else {
			resp.Error = err.Error()
		}

		errResp := sc.response(conn, resp)

		if nil != errResp || nil != err || finish {
			return
		}
	}
}

func (sc *scanner) makeCachekvs(table string, kvs map[string]*kv) {
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

			sc.kv = append(sc.kv, ckv)
			sc.exclude = append(sc.exclude, v.key)
		}
	}
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

func (sc *scanner) next(scanner *sql.Scanner, count int) (rows []*flyproto.Row, finish bool, err error) {
	if 0 >= count {
		count = 200
	} else if count > 200 {
		count = 200
	}

	var r []*sql.ScannerRow

	if r, err = scanner.Next(count); nil != err {
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

	for count > 0 && sc.offset < len(sc.kv) {
		rows = append(rows, &flyproto.Row{
			Key:     sc.kv[sc.offset].key,
			Version: sc.kv[sc.offset].version,
			Fields:  sc.fillDefault(sc.kv[sc.offset].fields),
		})
		count--
		sc.offset++
	}

	if len(r) == 0 && sc.offset >= len(sc.kv) {
		finish = true
	}

	return
}
