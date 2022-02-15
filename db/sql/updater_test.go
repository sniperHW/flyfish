package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestUpdaterPgSql
//go tool cover -html=coverage.out
import (
	"fmt"
	//"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

/*
type UpdateState struct {
	Version              int64 //最新版本号
	Key                  string
	Slot                 int
	Fields               map[string]*proto.Field
	Meta                 TableMeta
	State                DBState
	LastWriteBackVersion int64 //上次回写时的版本号
}
*/

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

func TestUpdaterPgSql(t *testing.T) {
	m, _ := CreateDbMetaFromJson([]byte(metaJson))
	dbc, _ := SqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")

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

	updater := NewUpdater(dbc, "pgsql", nil)

	updater.Start()

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(1))

	update.ch = make(chan interface{})
	update.state.Version = 0
	update.state.State = db.DBState_delete

	updater.IssueUpdateTask(update)
	<-update.ch

	//再次插入
	update.ch = make(chan interface{})
	update.state.Version = 10
	update.state.State = db.DBState_insert
	update.state.LastWriteBackVersion = 1

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(10))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "111111")
	update.state.Version++
	update.state.State = db.DBState_update
	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(11))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "2222")
	update.state.Version++
	update.state.State = db.DBState_update
	update.state.LastWriteBackVersion = 10
	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(12))

	updater.Stop()
}

func TestUpdaterMySql(t *testing.T) {
	m, _ := CreateDbMetaFromJson([]byte(metaJson))
	dbc, _ := SqlOpen("mysql", "localhost", 3306, "test", "root", "12345678")

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

	updater := NewUpdater(dbc, "mysql", nil)

	updater.Start()

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(1))

	update.ch = make(chan interface{})
	update.state.Version = 0
	update.state.State = db.DBState_delete

	updater.IssueUpdateTask(update)
	<-update.ch

	//再次插入
	update.ch = make(chan interface{})
	update.state.Version = 10
	update.state.State = db.DBState_insert
	update.state.LastWriteBackVersion = 1

	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(10))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "111111")
	update.state.Version++
	update.state.State = db.DBState_update
	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(11))

	update.ch = make(chan interface{})
	update.state.Fields["phone"] = proto.PackField("phone", "2222")
	update.state.Version++
	update.state.State = db.DBState_update
	update.state.LastWriteBackVersion = 10
	updater.IssueUpdateTask(update)
	<-update.ch

	assert.Equal(t, update.state.LastWriteBackVersion, int64(12))

	updater.Stop()
}
