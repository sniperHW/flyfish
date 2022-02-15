package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestUpdaterPgSql
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

type updateTask struct {
	state db.UpdateState
	ch    chan interface{}
}

func (u *updateTask) CheckUpdateLease() bool {
	return true
}

func (u *updateTask) Dirty() bool {
	return false
}

func (u *updateTask) GetUniKey() string {
	return fmt.Sprintf("%s:%s", u.state.Meta.TableName(), u.state.Key)
}

func (u *updateTask) GetTable() string {
	return u.state.Meta.TableName()
}

func (u *updateTask) SetLastWriteBackVersion(version int64) {
	u.state.LastWriteBackVersion = version
}

func (u *updateTask) GetUpdateAndClearUpdateState() db.UpdateState {
	return u.state
}

func (u *updateTask) ReleaseLock() {
	close(u.ch)
}

func (u *updateTask) ClearUpdateStateAndReleaseLock() {
	close(u.ch)
}

var metaJson string = `
{
	"TableDefs":[
	{"Name":"users1",
	 "Fields":[
	 	{"Name":"name","Type":"string","DefaultValue":""},
	 	{"Name":"age","Type":"int","DefaultValue":"0"},
	 	{"Name":"phone","Type":"string","DefaultValue":""}]
	}]
}  
`

type loadTask struct {
	key   string
	meta  db.TableMeta
	retch chan []interface{}
}

func (l *loadTask) GetKey() string {
	return l.key
}

func (l *loadTask) GetUniKey() string {
	return fmt.Sprintf("%s:%s", l.meta.TableName(), l.key)
}

func (l *loadTask) GetTable() string {
	return l.meta.TableName()
}

func (l *loadTask) OnResult(err error, version int64, fields map[string]*proto.Field) {
	l.retch <- []interface{}{err, version, fields}
}

func (l *loadTask) GetTableMeta() db.TableMeta {
	return l.meta
}

func load(l *loader, meta db.TableMeta, key string) (err error, version int64, fields map[string]*proto.Field) {
	task := &loadTask{
		key:   key,
		meta:  meta,
		retch: make(chan []interface{}, 1),
	}

	l.IssueLoadTask(task)

	ret := <-task.retch

	if nil != ret[0] {
		err = ret[0].(error)
	}

	version = ret[1].(int64)
	fields = ret[2].(map[string]*proto.Field)
	return
}

func testUpdater(t *testing.T, dbc *sqlx.DB, sqlType string) {
	m, _ := CreateDbMetaFromJson([]byte(metaJson))

	dbc.Exec("delete from users1_0 where __key__ = 'test1';")

	update := &updateTask{
		state: db.UpdateState{
			Version: 1,
			Key:     "test1",
			Slot:    0,
			Meta:    m.GetTableMeta("users1"),
			State:   db.DBState_insert,
			Fields:  map[string]*proto.Field{},
		},
	}

	update.state.Fields["name"] = proto.PackField("name", "test1")
	update.state.Fields["age"] = proto.PackField("age", 1)
	update.state.Fields["phone"] = proto.PackField("phone", "8763051")
	update.ch = make(chan interface{})

	updater := NewUpdater(dbc, sqlType, nil)

	updater.Start()

	fmt.Println("insert")

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(1))

	update.ch = make(chan interface{})
	update.state.Version = -2
	update.state.State = db.DBState_delete

	fmt.Println("delete")

	updater.IssueUpdateTask(update)
	<-update.ch

	//再次插入
	update.ch = make(chan interface{})
	update.state.Version = 3
	update.state.State = db.DBState_insert
	update.state.LastWriteBackVersion = 1

	fmt.Println("update with version mismatch")

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(3))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "111111")
	update.state.Version++
	update.state.State = db.DBState_update

	fmt.Println("update with version match")

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(4))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "2222")
	//update.state.Version
	update.state.State = db.DBState_update
	update.state.LastWriteBackVersion = 1

	fmt.Println("update with version mismatch,drop update")

	updater.IssueUpdateTask(update)
	<-update.ch

	//assert.Equal(t, update.state.LastWriteBackVersion, int64(12))

	loader := NewLoader(dbc, 1, 1)
	loader.Start()

	err, version, fields := load(loader, m.GetTableMeta("users1"), "test1")

	assert.Nil(t, err)
	assert.Equal(t, version, int64(4))
	assert.Equal(t, "111111", fields["phone"].GetString())

	loader.Stop()
	updater.Stop()

}

func TestUpdaterPgSql(t *testing.T) {
	dbc, _ := SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")
	testUpdater(t, dbc, "pgsql")

}

func TestUpdaterMySql(t *testing.T) {
	dbc, _ := SqlOpen("mysql", "localhost", 3306, "test", "root", "12345678")
	testUpdater(t, dbc, "mysql")
}
