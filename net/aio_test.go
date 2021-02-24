// +build aio

//go test -tags=aio -covermode=count -v -coverprofile=coverage.out -run=TestAio
//go tool cover -html=coverage.out
package net

import (
	"github.com/sniperHW/flyfish/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func MakeSetRequest(table, key string, fields map[string]interface{}) *Message {

	pbdata := &protocol.SetReq{}

	for k, v := range fields {
		pbdata.Fields = append(pbdata.Fields, protocol.PackField(k, v))
	}

	return NewMessage(CommonHead{
		Seqno:  1,
		UniKey: table + ":" + key,
	}, pbdata)

}

func TestAio(t *testing.T) {
	var err error

	e := NewEncoder(pb.GetNamespace("request"), false)

	fields := map[string]interface{}{}
	fields["name"] = "sniperHW"

	m, err := e.EnCode(MakeSetRequest("test", "test", fields))
	assert.Nil(t, err)

	//一次解出包

	bytes := m.Bytes()

	r := NewReceiver(pb.GetNamespace("request"), false)

	r.OnData(bytes)

	msg, err := r.Unpack()
	assert.Nil(t, err)

	assert.Equal(t, int64(1), msg.(*Message).head.Seqno)
	assert.Equal(t, "test:test", msg.(*Message).head.UniKey)
	assert.Equal(t, r.w, r.r)

	//两次解出包

	buff := make([]byte, 1024)

	half := len(bytes) / 2
	copy(buff, bytes[:half])

	r.OnData(buff[:half])
	msg, err = r.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, uint64(0), r.r)
	assert.Equal(t, uint64(half), r.w)

	buff = r.GetRecvBuff()

	copy(buff, bytes[half:])

	r.OnData(buff)
	msg, err = r.Unpack()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), msg.(*Message).head.Seqno)
	assert.Equal(t, "test:test", msg.(*Message).head.UniKey)
	assert.Equal(t, r.w, r.r)

}
