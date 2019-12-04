package pb

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"reflect"
)

type reflectInfo struct {
	tt   reflect.Type
	name string
}

type Namespace struct {
	nameToID map[string]uint32
	idToMeta map[uint32]reflectInfo
}

func (this *Namespace) newMessage(id uint32) (msg proto.Message, err error) {
	if mt, ok := this.idToMeta[id]; ok {
		msg = reflect.New(mt.tt.Elem()).Interface().(proto.Message)
	} else {
		err = fmt.Errorf("invaild id:%d", id)
	}
	return
}

func (this *Namespace) GetNameByID(id uint32) string {
	if mt, ok := this.idToMeta[id]; ok {
		return mt.name
	} else {
		return ""
	}
}

//根据名字注册实例(注意函数非线程安全，需要在初始化阶段完成所有消息的Register)
func (this *Namespace) Register(msg proto.Message, id uint32) error {
	tt := reflect.TypeOf(msg)
	name := tt.String()

	if _, ok := this.nameToID[name]; ok {
		return fmt.Errorf("%s already register", name)
	}

	this.nameToID[name] = id
	this.idToMeta[id] = reflectInfo{tt: tt, name: name}
	return nil
}

func (this *Namespace) Marshal(o interface{}) ([]byte, uint32, error) {
	var id uint32
	var ok bool

	if id, ok = this.nameToID[reflect.TypeOf(o).String()]; !ok {
		return nil, 0, fmt.Errorf("unregister type:%s", reflect.TypeOf(o).String())
	}

	msg := o.(proto.Message)

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, 0, err
	}
	return data, id, nil
}

func (this *Namespace) Unmarshal(id uint32, buff []byte) (proto.Message, error) {
	var msg proto.Message
	var err error

	if msg, err = this.newMessage(id); err != nil {
		return nil, err
	}

	if nil == buff || len(buff) == 0 {
		//返回默认消息体
		return msg, nil
	}

	if err = proto.Unmarshal(buff, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

var requestSpace *Namespace
var responseSpace *Namespace

func GetNamespace(space string) *Namespace {
	if space == "request" {
		return requestSpace
	} else if space == "response" {
		return responseSpace
	} else {
		return nil
	}
}

func init() {

	requestSpace = &Namespace{
		nameToID: map[string]uint32{},
		idToMeta: map[uint32]reflectInfo{},
	}

	responseSpace = &Namespace{
		nameToID: map[string]uint32{},
		idToMeta: map[uint32]reflectInfo{},
	}

}
