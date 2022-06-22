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

var vaildChar [255]bool

func init() {
	for c := byte('a'); c <= byte('z'); c++ {
		vaildChar[int(c)] = true
	}

	for c := byte('A'); c <= byte('Z'); c++ {
		vaildChar[int(c)] = true
	}

	for c := byte('0'); c <= byte('9'); c++ {
		vaildChar[int(c)] = true
	}

	vaildChar[int(byte('_'))] = true

}

func ContainsInvaildCharacter(s string) bool {
	for _, v := range s {
		if !vaildChar[int(v)] {
			return true
		}
	}
	return false
}

/*对于string类型如果长度可能超过16384则使用blob,否则在mysql下将会截断*/

type FieldDef struct {
	TabVersion   int64  //TableDef.Version when field create
	Name         string `json:"Name,omitempty"`
	Type         string `json:"Type,omitempty"`
	DefaultValue string `json:"DefaultValue,omitempty"` //blob类型在mysql无法设置默认值，因此，blob类型统一忽略用户设定的默认值，使用空串作为默认值
}

func (f *FieldDef) GetRealName() string {
	return fmt.Sprintf("%s_%d", f.Name, f.TabVersion)
}

func (f *FieldDef) GetProtoType() proto.ValueType {
	switch f.Type {
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

func (f *FieldDef) GetDefaultValueStr() string {
	if "" != f.DefaultValue {
		return f.DefaultValue
	} else {
		switch f.Type {
		case "int":
			return "0"
		case "float":
			return "0.0"
		case "string":
			return ""
		case "blob":
			return ""
		default:
			return ""
		}
	}
}

func (f *FieldDef) GetDefaultValue() interface{} {
	tt := f.GetProtoType()
	v := f.GetDefaultValueStr()
	switch tt {
	case proto.ValueType_string:
		return v
	case proto.ValueType_int:
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
	case proto.ValueType_float:
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
	case proto.ValueType_blob:
		return []byte{}
	default:
		return nil
	}
}

type TableDef struct {
	DbVersion int64       `json:"DbVersion,omitempty"` //DbDef.Version when table create
	Version   int64       `json:"Version,omitempty"`
	Name      string      `json:"Name,omitempty"`
	Fields    []*FieldDef `json:"Fields,omitempty"`
}

func (t *TableDef) GetField(n string) *FieldDef {
	for i, v := range t.Fields {
		if v.Name == n {
			return t.Fields[i]
		}
	}
	return nil
}

func (t *TableDef) GetRealName() string {
	return fmt.Sprintf("%s_%d", t.Name, t.DbVersion)
}

func (t *TableDef) Clone() *TableDef {
	ret := TableDef{
		Name:      t.Name,
		DbVersion: t.DbVersion,
		Version:   t.Version,
	}

	for _, v := range t.Fields {
		ret.Fields = append(ret.Fields, &FieldDef{
			TabVersion:   v.TabVersion,
			Name:         v.Name,
			Type:         v.Type,
			DefaultValue: v.DefaultValue,
		})
	}

	return &ret
}

func (t *TableDef) Check() error {
	names := map[string]bool{}
	if t.Name == "" {
		return errors.New("table name is empty")
	}

	if ContainsInvaildCharacter(t.Name) {
		return errors.New("table name contains invalid character")
	}

	for _, v := range t.Fields {
		if v.Name == "" {
			return fmt.Errorf("%s has emtpy filed.Name", t.Name)
		}

		if ContainsInvaildCharacter(v.Name) {
			return fmt.Errorf("%s has invaild filed.Name:%s", t.Name, v.Name)
		}

		if names[v.Name] {
			return fmt.Errorf("%s has duplicate filed.Name:%s", t.Name, v.Name)
		}

		if v.GetProtoType() == proto.ValueType_invaild {
			return fmt.Errorf("%s has invaild filed.Type:%s", t.Name, v.Type)
		}

		if nil == v.GetDefaultValue() {
			return fmt.Errorf("%s filed.Type:%s invaild DefaultValue:%s", t.Name, v.Type, v.DefaultValue)
		}

		names[v.Name] = true
	}

	return nil
}

func (t *TableDef) RemoveField(name string) bool {
	for k, v := range t.Fields {
		if v.Name == name {
			last := len(t.Fields) - 1
			t.Fields[k], t.Fields[last] = t.Fields[last], t.Fields[k]
			t.Fields = t.Fields[:last]
			return true
		}
	}
	return false
}

type DbDef struct {
	Version   int64
	TableDefs []*TableDef
}

func (d *DbDef) Check() error {
	tbs := map[string]bool{}
	for _, v := range d.TableDefs {
		if _, ok := tbs[v.Name]; ok {
			return fmt.Errorf("duplicate table %s", v.Name)
		}

		if err := v.Check(); nil != err {
			return err
		}

		tbs[v.Name] = true
	}

	return nil
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
	ret := &DbDef{
		Version: d.Version,
	}

	for _, v := range d.TableDefs {
		ret.TableDefs = append(ret.TableDefs, v.Clone())
	}
	return ret
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

func MakeDbDefFromJsonString(s []byte) (*DbDef, error) {
	var def DbDef
	if err := json.Unmarshal(s, &def); nil != err {
		return nil, err
	} else {
		if err = def.Check(); nil != err {
			return nil, err
		} else {
			return &def, nil
		}
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
	CheckFieldWithVersion(string, int64) bool
	TableName() string
	FillDefaultValues(fields map[string]*proto.Field)
	GetDef() *TableDef
}

type DBLoadTask interface {
	GetTable() string
	GetKey() string
	GetUniKey() string
	GetTableMeta() TableMeta
	OnResult(err error, version int64, fields map[string]*proto.Field)
}

type UpdateState struct {
	Version              int64 //最新版本号
	Key                  string
	Slot                 int
	Fields               map[string]*proto.Field
	Meta                 TableMeta
	State                DBState
	LastWriteBackVersion int64 //上次回写时的版本号
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
	SetLastWriteBackVersion(int64)
	OnError(error)
}
