package db

import (
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync"
)

type kv struct {
	uniKey  string
	version int64
	fields  map[string]*flyproto.Field //字段
}

type DB struct {
	store     map[string]*kv
	que       *queue.ArrayQueue
	stoponce  sync.Once
	startonce sync.Once
}

func (d *DB) do(v interface{}) {
	switch v.(type) {
	case db.DBLoadTask:
		t := v.(db.DBLoadTask)
		kv, ok := d.store[t.GetUniKey()]
		if ok {
			t.OnResult(nil, kv.version, kv.fields)
		} else {
			t.OnResult(db.ERR_RecordNotExist, 0, nil)
		}
	case db.DBUpdateTask:
		t := v.(db.DBUpdateTask)
		if !t.CheckUpdateLease() {
			t.ClearUpdateStateAndReleaseLock() //释放更新锁定
		} else {
			s := t.GetUpdateAndClearUpdateState()

			//构造更新语句
			switch s.State {
			case db.DBState_insert:
				k, ok := d.store[t.GetUniKey()]
				if ok {
					k.version = s.Version
					for n, vv := range s.Fields {
						k.fields[n] = vv
					}
				} else {
					k := &kv{
						uniKey:  t.GetUniKey(),
						version: s.Version,
						fields:  map[string]*flyproto.Field{},
					}
					for n, vv := range s.Fields {
						k.fields[n] = vv
					}
					d.store[k.uniKey] = k
				}
			case db.DBState_update:
				k, ok := d.store[t.GetUniKey()]
				if ok {
					k.version = s.Version
					for n, vv := range s.Fields {
						k.fields[n] = vv
					}
				} else {
					panic("invaild State")
				}
			case db.DBState_delete:
				delete(d.store, t.GetUniKey())
			default:
				panic("invaild State")
				t.ReleaseLock()
				return
			}

			if t.Dirty() {
				//再次发生变更,插入队列继续执行
				d.que.ForceAppend(t)
			} else {
				t.ReleaseLock()
			}

		}
	}
}

func (d *DB) Stop() {
	d.stoponce.Do(func() {
		d.que.Close()
	})
}

func (d *DB) Start() {
	d.startonce.Do(func() {
		go func() {
			localList := make([]interface{}, 0, 200)
			closed := false
			for {

				localList, closed = d.que.Pop(localList)
				size := len(localList)
				if closed && size == 0 {
					break
				}

				for i, v := range localList {
					d.do(v)
					localList[i] = nil
				}
			}
		}()
	})
}

func (d *DB) IssueTask(t interface{}) error {
	return d.que.Append(t)
}

func New() *DB {
	return &DB{
		store: map[string]*kv{},
		que:   queue.NewArrayQueue(1000),
	}
}
