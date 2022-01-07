package flykv

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/buffer"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"net"
	"time"
)

func send(conn net.Conn, msg proto.Message, timeout time.Duration) error {
	b := buffer.Get()
	defer b.Free()
	data, _ := proto.Marshal(msg)
	b.AppendUint32(uint32(len(data)))
	b.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := conn.Write(b.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return err
}

func recv(conn net.Conn, maxbuff int, msg proto.Message, timeout time.Duration) error {
	b := make([]byte, maxbuff)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := conn.Read(b[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return err
		}

		w = w + n

		if w >= 4 {
			pbsize = int(binary.BigEndian.Uint32(b[:4]))
		}

		if pbsize > len(b)-4 {
			return errors.New("invaild packet")
		}

		if w >= pbsize+4 {
			if err = proto.Unmarshal(b[4:w], msg); err == nil {
				return nil
			} else {
				return err
			}
		}
	}
}

func SendScanerResp(conn net.Conn, resp *flyproto.MakeScannerResp) error {
	return send(conn, resp, time.Second*5)
}

func RecvScanerReq(conn net.Conn) (*flyproto.MakeScanerReq, error) {
	req := &flyproto.MakeScanerReq{}
	err := recv(conn, 1024, req, time.Second*5)
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
	config     *Config
	slot       int
	table      string
}

func (this *kvnode) onScanner(session *fnet.Socket) {
	go func() {
		//GetSugar().Infof("onScanner")
		conn := session.GetUnderConn()
		req, err := RecvScanerReq(conn)
		if nil != err {
			conn.Close()
			return
		}

		var store *kvstore
		var ok bool

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

		if !ok {
			SendScanerResp(conn, &flyproto.MakeScannerResp{
				Ok:     false,
				Reason: "store not found",
			})
			conn.Close()
			return
		} else if !store.isLeader() {
			SendScanerResp(conn, &flyproto.MakeScannerResp{
				Ok:     false,
				Reason: "store is not leader",
			})
			conn.Close()
			return
		}

		tbmeta := store.meta.GetTableMeta(req.Table)
		if nil == tbmeta {
			SendScanerResp(conn, &flyproto.MakeScannerResp{
				Ok:     false,
				Reason: "store not found",
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
			config:     this.config,
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
			this.muC.Lock()
			this.clients[session] = struct{}{}
			this.muC.Unlock()
			go scanner.loop(this, session)
		}
	}()
}

func (sc *scanner) recv(conn net.Conn) (*flyproto.ScanReq, error) {
	req := &flyproto.ScanReq{}
	err := recv(conn, 8192, req, time.Second*30)
	return req, err
}

func (sc *scanner) response(conn net.Conn, resp *flyproto.ScanResp) error {
	return send(conn, resp, time.Second*5)
}

func (sc *scanner) loop(kvnode *kvnode, session *fnet.Socket) {
	//GetSugar().Infof("scanner.loop slot:%d", sc.slot)
	conn := session.GetUnderConn()

	dbConfig := sc.config.DBConfig

	dbc, err := sqlOpen(sc.config.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DB, dbConfig.User, dbConfig.Password)
	if nil != err {
		sc.response(conn, &flyproto.ScanResp{Error: err.Error()})
		conn.Close()
		return
	}

	scanner, err := sql.NewScanner(sc.tbmeta, dbc, sc.slot, sc.table, sc.wantFields, sc.exclude)

	if nil != err {
		sc.response(conn, &flyproto.ScanResp{Error: err.Error()})
		conn.Close()
		return
	}

	defer func() {
		scanner.Close()
		dbc.Close()
		kvnode.muC.Lock()
		delete(kvnode.clients, session)
		kvnode.muC.Unlock()
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
			count--
		}
	}

	for count > 0 && sc.offset < len(sc.kv) {
		rows = append(rows, &flyproto.Row{
			Key:     sc.kv[sc.offset].key,
			Version: sc.kv[sc.offset].version,
			Fields:  sc.fillDefault(sc.kv[sc.offset].fields),
		})
		count--
		sc.offset++
	}

	if count > 0 && sc.offset >= len(sc.kv) {
		finish = true
	}

	return
}
