package pb

import (
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
)

type reflectInfo struct {
	tt   reflect.Type
	name string
}

var nameToID = map[string]uint32{}
var idToMeta = map[uint32]reflectInfo{}

func newMessage(id uint32) (msg proto.Message, err error) {
	if mt, ok := idToMeta[id]; ok {
		msg = reflect.New(mt.tt.Elem()).Interface().(proto.Message)
	} else {
		err = fmt.Errorf("invaild id:%d", id)
	}
	return
}

func GetNameByID(id uint32) string {
	if mt, ok := idToMeta[id]; ok {
		return mt.name
	} else {
		return ""
	}
}

//根据名字注册实例(注意函数非线程安全，需要在初始化阶段完成所有消息的Register)
func Register(msg proto.Message, id uint32) error {
	tt := reflect.TypeOf(msg)
	name := tt.String()

	if _, ok := nameToID[name]; ok {
		return fmt.Errorf("%s already register", name)
	}

	nameToID[name] = id
	idToMeta[id] = reflectInfo{tt: tt, name: name}
	return nil
}

func Marshal(o interface{}) ([]byte, uint32, error) {
	var id uint32
	var ok bool

	if id, ok = nameToID[reflect.TypeOf(o).String()]; !ok {
		return nil, 0, fmt.Errorf("unregister type:%s", reflect.TypeOf(o).String())
	}

	msg := o.(proto.Message)

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, 0, err
	}
	return data, id, nil
}

func Unmarshal(id uint32, buff []byte) (proto.Message, error) {
	var msg proto.Message
	var err error

	if msg, err = newMessage(id); err != nil {
		return nil, err
	}

	if err = proto.Unmarshal(buff, msg); err != nil {
		return nil, err
	}

	return msg, nil
}
