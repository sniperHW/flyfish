package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"github.com/sniperHW/flyfish/proto/cs/scan"
	"net"
	"time"
)

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
	conn       net.Conn
}

func (this *kvnode) onScanner(conn net.Conn) {
	go func() {
		req, err := scan.RecvScannerReq(conn, time.Now().Add(time.Second*5))
		if nil != err {
			conn.Close()
			return
		}

		deadline := time.Now().Add(time.Duration(req.Timeout))

		var store *kvstore
		var ok bool
		var tbmeta db.TableMeta

		this.muS.RLock()
		store, ok = this.stores[int(req.Store)]
		this.muS.RUnlock()

		var wantSlots *bitmap.Bitmap
		var fields []string

		errCode := func() int {
			if !ok {
				return scan.Err_invaild_store
			} else if !store.isReady() {
				return scan.Err_store_not_ready
			}

			tbmeta = store.meta.GetTableMeta(req.Table)

			if nil == tbmeta || tbmeta.GetDef().DbVersion != req.Version {
				//表不存在，或删除后又添加，已经不符合请求要求的版本
				return scan.Err_invaild_table
			}

			for _, v := range req.Fields {
				if tbmeta.CheckFieldWithVersion(v.Field, v.Version) {
					fields = append(fields, v.Field)
				} else {
					//字段不存在，或删除后又添加，已经不符合请求要求的版本
					return scan.Err_invaild_field
				}
			}

			if wantSlots, err = bitmap.CreateFromJson(req.Slots); nil != err {
				return scan.Err_unpack
			}

			return scan.Err_ok

		}()

		if time.Now().After(deadline) {
			conn.Close()
			return
		}

		if errCode != scan.Err_ok {
			scan.SendScannerResp(conn, errCode, time.Now().Add(time.Second))
			conn.Close()
			return
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
					scanner.makeCachekvs(v, store.slotsKvMap[v])
				}
			}
			close(ch)
		})

		<-ch

		if err = scan.SendScannerResp(conn, scan.Err_ok, time.Now().Add(time.Second)); nil != err {
			conn.Close()
		} else {
			go scanner.loop(this, conn)
		}
	}()
}

func (sc *scanner) loop(kvnode *kvnode, conn net.Conn) {
	defer func() {
		conn.Close()
		if nil != sc.scanner {
			sc.scanner.Close()
		}
	}()
	for {
		req, err := scan.RecvScanNextReq(conn, time.Now().Add(scan.RecvScanNextReqTimeout))
		if nil != err {
			return
		}

		if breakloop := sc.next(kvnode, conn, int(req.Count), time.Now().Add(time.Duration(req.Timeout))); breakloop {
			return
		}
	}
}

func (sc *scanner) next(kvnode *kvnode, conn net.Conn, count int, deadline time.Time) (breakloop bool) {
	resp := &flyproto.ScanNextResp{}
	defer func() {
		if time.Now().After(deadline) {
			breakloop = true
		} else if nil != cs.Send(conn, resp, time.Now().Add(time.Second)) {
			breakloop = true
		} else {
			breakloop = breakloop || resp.ErrCode != int32(scan.Err_ok)
		}
	}()

	if sc.offset >= len(sc.slots) {
		//插入哑元标识store遍历结束
		resp.Rows = append(resp.Rows, scan.MakeDummyRow(scan.DummyStore))
		breakloop = true
		return
	}

	if 0 >= count {
		count = 200
	} else if count > 200 {
		count = 200
	}

	var err error

	s := sc.slots[sc.offset]
	resp.Slot = int32(s.slot)

	if nil == sc.scanner {
		sc.scanner, err = sql.NewScanner(sc.tbmeta, kvnode.dbc, s.slot, sc.wantFields, s.exclude)
		if nil != err {
			resp.ErrCode = int32(scan.Err_db)
			return
		}
	}

	var r []*sql.ScannerRow

	if r, err = sc.scanner.Next(count); nil != err {
		resp.ErrCode = int32(scan.Err_db)
		return
	} else {
		for _, v := range r {
			resp.Rows = append(resp.Rows, &flyproto.Row{
				Key:     v.Key,
				Version: v.Version,
				Fields:  sc.fillDefault(v.Fields),
			})
		}
	}

	count -= len(r)

	for count > 0 && s.offset < len(s.kv) {
		resp.Rows = append(resp.Rows, &flyproto.Row{
			Key:     s.kv[s.offset].key,
			Version: s.kv[s.offset].version,
			Fields:  sc.fillDefault(s.kv[s.offset].fields),
		})
		count--
		s.offset++
	}

	if len(r) == 0 && s.offset >= len(s.kv) {
		//插入哑元标识slot遍历结束
		resp.Rows = append(resp.Rows, scan.MakeDummyRow(scan.DummySlot))
		sc.offset++
		sc.scanner.Close()
		sc.scanner = nil
	}

	return
}

func (sc *scanner) makeCachekvs(slotNo int, kvs map[string]*kv) {

	s := &slot{
		slot: slotNo,
	}

	if nil != kvs {
		for _, v := range kvs {
			if v.state == kv_ok {
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

	//GetSugar().Infof("makeCachekvs:%d kv count:%d", slotNo, len(s.kv))

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
