package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
)

var (
	ERR_DbError        = errors.New("db error")
	ERR_RecordNotExist = errors.New("record not exist")
)

type DBState byte

const (
	DBState_none   = DBState(0)
	DBState_insert = DBState(1)
	DBState_update = DBState(2)
	DBState_delete = DBState(3)
)

type FieldDef struct {
	Name        string `json:"Name,omitempty"`
	Type        string `json:"Type,omitempty"`
	DefautValue string `json:"DefautValue,omitempty"`
}

type TableDef struct {
	Version int64
	Name    string      `json:"Name,omitempty"`
	Fields  []*FieldDef `json:"Fields,omitempty"`
}

func (t *TableDef) GetField(n string) *FieldDef {
	for i, v := range t.Fields {
		if v.Name == n {
			return t.Fields[i]
		}
	}
	return nil
}

func (t *TableDef) GetRealFieldName(name string) string {
	return fmt.Sprintf("%s_%d", name, t.Version)
}

func (t *TableDef) Clone() *TableDef {
	ret := TableDef{
		Name: t.Name,
	}

	for _, v := range t.Fields {
		ret.Fields = append(ret.Fields, v)
	}

	return &ret
}

type DbDef struct {
	Version   int64
	TableDefs []*TableDef
}

func (d *DbDef) GetTableDef(n string) *TableDef {
	for i, v := range d.TableDefs {
		if v.Name == n {
			return d.TableDefs[i]
		}
	}
	return nil
}

func (d *DbDef) Clone() *DbDef {
	var ret DbDef
	for _, v := range d.TableDefs {
		ret.TableDefs = append(ret.TableDefs, v.Clone())
	}
	return &ret
}

func (d *DbDef) ToJson() ([]byte, error) {
	if data, err := json.Marshal(d); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (d *DbDef) ToPrettyJson() ([]byte, error) {
	if data, err := json.MarshalIndent(d, "", "    "); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (d *DbDef) GetRealTableName(name string) string {
	return fmt.Sprintf("%s_%d", name, d.Version)
}

func MakeDbDefFromJsonString(s []byte) (*DbDef, error) {
	var def DbDef
	if err := json.Unmarshal(s, &def); nil != err {
		return nil, err
	} else {
		return &def, nil
	}
}

func GetTypeByStr(s string) proto.ValueType {
	switch s {
	case "int":
		return proto.ValueType_int
	case "float":
		return proto.ValueType_float
	case "string":
		return proto.ValueType_string
	case "blob":
		return proto.ValueType_blob
	default:
		return proto.ValueType_invaild
	}
}

func GetDefaultValue(tt proto.ValueType, v string) interface{} {
	if tt == proto.ValueType_string {
		return v
	} else if tt == proto.ValueType_int {
		if v == "" {
			return int64(0)
		} else {
			i, err := strconv.ParseInt(v, 10, 64)
			if nil != err {
				return nil
			} else {
				return i
			}
		}
	} else if tt == proto.ValueType_float {
		if v == "" {
			return float64(0)
		} else {
			f, err := strconv.ParseFloat(v, 64)
			if nil != err {
				return nil
			} else {
				return f
			}
		}
	} else if tt == proto.ValueType_blob {
		return []byte{}
	} else {
		return nil
	}
}

type DBMeta interface {
	GetTableMeta(tab string) TableMeta
	CheckTableMeta(tab TableMeta) TableMeta //如果tab与DBMeta版本一致，直接返回tab否则返回最新的TableMeta
	GetVersion() int64
	ToJson() ([]byte, error)
	ToPrettyJson() ([]byte, error)
	GetDef() *DbDef
	MoveTo(DBMeta)
}

type TableMeta interface {
	GetDefaultValue(name string) interface{}
	CheckFields(fields ...*proto.Field) error
	CheckFieldsName([]string) error
	GetAllFieldsName() []string
	TableName() string
	FillDefaultValues(fields map[string]*proto.Field)
}

type DBLoadTask interface {
	GetTable() string
	GetKey() string
	GetUniKey() string
	GetTableMeta() TableMeta
	OnResult(err error, version int64, fields map[string]*proto.Field)
}

type UpdateState struct {
	Version int64
	Key     string
	Slot    int
	Fields  map[string]*proto.Field
	Meta    TableMeta
	State   DBState
}

type DBLoader interface {
	IssueLoadTask(DBLoadTask) error
	Start()
	Stop()
}

type DBUpdater interface {
	IssueUpdateTask(DBUpdateTask) error
	Start()
	Stop()
}

/*
updateTask实现


应用层产生更新后

为task设置正确的DBState，将需要更新的字段添加到task的Fields中

检查task的Lock标记，如果未锁定，表明回写任务没有在执行，设置Lock标记并将task提交到updater。

在updater真正执行之前，task可能因为多次的变更而改变更新状态，updater会以执行时的实际状态产生更新语句。


updater线程

从执行队列取出task

调用task.GetUpdateAndClearUpdateState 获取变更状态,同时将变更状态重置为空（不释放Lock）

updater使用更新状态产生sql语句并执行。

sql执行完毕

调用task.Dirty以检查在执行期间task是否再次发生变更

如果是则把task再次投入到执行队列

否则,释放Lock


*/

type DBUpdateTask interface {
	CheckUpdateLease() bool //返回是否持有更新租约，如果返回false将不能执行update
	ReleaseLock()           //解除更新锁
	Dirty() bool            //是否脏的
	ClearUpdateStateAndReleaseLock()
	GetUpdateAndClearUpdateState() UpdateState //获取脏状态同时将其清理
	GetUniKey() string
	GetTable() string
}
