package str

import (
	"github.com/sniperHW/flyfish/proto"
	"testing"
)

func TestStr(t *testing.T) {
	str := Get()

	str.AppendInt64(100)

	if str.len != 8 {
		t.Fatal("str.len != 8")
	}

	i64, err := str.ReadInt64(0)

	if nil != err || i64 != 100 {
		t.Fatal("get i64 failed")
	}

	f1 := proto.PackField("int", 100)
	f2 := proto.PackField("uint", uint64(200))
	f3 := proto.PackField("float", 0.1)
	f4 := proto.PackField("string", "hello")
	f5 := proto.PackField("blog", []byte("world"))

	str = Get()

	str.AppendField(f1).AppendField(f2).AppendField(f3).AppendField(f4).AppendField(f5)

	offset := 0

	f1, offset, err = str.ReadField(offset)

	if nil != err {
		t.Fatal("get f1 failed")
	}

	if f1.GetInt() != 100 {
		t.Fatal("get f1 failed")
	}

	f2, offset, err = str.ReadField(offset)

	if nil != err {
		t.Fatal("get f2 failed")
	}

	if f2.GetUint() != 200 {
		t.Fatal("get f2 failed")
	}

	f3, offset, err = str.ReadField(offset)

	if nil != err || f3 == nil {
		t.Fatal("get f3 failed")
	}

	if f3.GetFloat() != 0.1 {
		t.Fatal("get f3 failed")
	}

	f4, offset, err = str.ReadField(offset)

	if nil != err || f4 == nil {
		t.Fatal("get f4 failed")
	}

	if f4.GetString() != "hello" {
		t.Fatal("get f3 failed")
	}

	f5, offset, err = str.ReadField(offset)

	if nil != err || f5 == nil {
		t.Fatal("get f5 failed")
	}

	if string(f5.GetBlob()) != "world" {
		t.Fatal("get f5 failed")
	}

	str = NewStr(str.Bytes())

	offset = 0

	f1, offset, err = str.ReadField(offset)

	if nil != err {
		t.Fatal("get f1 failed")
	}

	if f1.GetInt() != 100 {
		t.Fatal("get f1 failed")
	}

	f2, offset, err = str.ReadField(offset)

	if nil != err {
		t.Fatal("get f2 failed")
	}

	if f2.GetUint() != 200 {
		t.Fatal("get f2 failed")
	}

	f3, offset, err = str.ReadField(offset)

	if nil != err || f3 == nil {
		t.Fatal("get f3 failed")
	}

	if f3.GetFloat() != 0.1 {
		t.Fatal("get f3 failed")
	}

	f4, offset, err = str.ReadField(offset)

	if nil != err || f4 == nil {
		t.Fatal("get f4 failed")
	}

	if f4.GetString() != "hello" {
		t.Fatal("get f3 failed")
	}

	f5, offset, err = str.ReadField(offset)

	if nil != err || f5 == nil {
		t.Fatal("get f5 failed")
	}

	if string(f5.GetBlob()) != "world" {
		t.Fatal("get f5 failed")
	}

}
