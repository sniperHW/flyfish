package mock

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/queue"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync"
	"sync/atomic"
)

type dbkv struct {
	uniKey  string
	version int64
	fields  map[string]*flyproto.Field //字段
}

type DB struct {
	sync.Mutex
	store     map[string]*dbkv
	que       *queue.ArrayQueue
	stoponce  int32
	startonce int32
}

func (d *DB) do(v interface{}) {
	d.Lock()
	defer d.Unlock()
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
					k := &dbkv{
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
					panic(fmt.Sprintf("invaild State:%s", t.GetUniKey()))
				}
			case db.DBState_delete:
				delete(d.store, t.GetUniKey())
			default:
				panic(fmt.Sprintf("invaild State:%s", t.GetUniKey()))
				t.ReleaseLock()
				return
			}

			t.SetLastWriteBackVersion(s.Version)

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
	if atomic.CompareAndSwapInt32(&d.stoponce, 0, 1) {
		d.que.Close()
	}
}

func (d *DB) Start() {
	if atomic.CompareAndSwapInt32(&d.startonce, 0, 1) {
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
	}
}

func (d *DB) IssueTask(t interface{}) error {
	return d.que.Append(t)
}

func (d *DB) Clone() *DB {
	d.Lock()
	defer d.Unlock()

	c := &DB{
		store: map[string]*dbkv{},
		que:   queue.NewArrayQueue(1000),
	}

	for k, v := range d.store {
		c.store[k] = v
	}

	return c
}

func NewDB() *DB {
	return &DB{
		store: map[string]*dbkv{},
		que:   queue.NewArrayQueue(1000),
	}
}
