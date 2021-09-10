package net

//go test -covermode=count -v -coverprofile=coverage.out -run=TestInbound

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func test_unpack(_ *pb.Namespace, buff []byte, r int, w int) (interface{}, int, error) {

	unpackSize := w - r
	if unpackSize >= 4 {
		reader := buffer.NewReader(buff[r : r+unpackSize])
		payload := int(reader.GetUint32())

		if payload == 0 {
			return nil, 0, fmt.Errorf("zero payload")
		}

		totalSize := payload + 4

		if totalSize <= unpackSize {
			msg, err := reader.CopyBytes(payload)
			return msg, totalSize, err
		} else {
			return nil, totalSize, nil
		}
	}
	return nil, 0, nil

}

func TestInbound(t *testing.T) {
	DefaultRecvBuffSize = 64
	un := NewInboundProcessor(test_unpack, nil)

	bu := *un.pBuffer

	binary.BigEndian.PutUint32(bu, 5)
	copy(bu[4:], "hello")
	fmt.Println(string(bu[4:9]), len(bu[:9]))
	un.OnData(bu[:9])

	assert.Equal(t, 0, un.r)

	assert.Equal(t, 9, un.w)

	msg, err := un.Unpack()

	assert.Nil(t, err)

	assert.Equal(t, "hello", string(msg.([]byte)))

	binary.BigEndian.PutUint32(bu, 124)
	un.OnData(bu[:9])

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	bu = *un.pBuffer
	assert.Equal(t, m512K, len(bu))

	bu = un.GetRecvBuff()

	copy(bu, strings.Repeat("l", 119))

	un.OnData(bu[:119])

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Equal(t, len(msg.([]byte)), 124)

	assert.Equal(t, len(*un.pBuffer), DefaultRecvBuffSize)

	bu = *un.pBuffer
	binary.BigEndian.PutUint32(bu, 5)
	copy(bu[4:], "hello")
	binary.BigEndian.PutUint32(bu[9:], 5)

	un.OnData(bu[:13])

	msg, err = un.Unpack()

	assert.Nil(t, err)

	assert.Equal(t, "hello", string(msg.([]byte)))

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, 4, un.w)

	copy(bu[4:], "world")
	un.OnData(bu[4:9])

	msg, err = un.Unpack()

	assert.Nil(t, err)

	assert.Equal(t, "world", string(msg.([]byte)))

	assert.Equal(t, 0, un.w)

	////////

	bu = *un.pBuffer
	binary.BigEndian.PutUint32(bu, m512K-4+1)

	un.OnData(bu[:4])

	assert.Equal(t, 4, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, len(*un.pBuffer), m1M)

	bu = un.GetRecvBuff()
	un.OnData(bu[:m512K-4+1])

	assert.Equal(t, m512K+1, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)

	assert.Equal(t, m512K-4+1, len(msg.([]byte)))

	////////

	bu = *un.pBuffer
	binary.BigEndian.PutUint32(bu, m1M-4+1)

	un.OnData(bu[:4])

	assert.Equal(t, 4, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, len(*un.pBuffer), m2M)

	bu = un.GetRecvBuff()
	un.OnData(bu[:m1M-4+1])

	assert.Equal(t, m1M+1, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)

	assert.Equal(t, m1M-4+1, len(msg.([]byte)))

	////////

	bu = *un.pBuffer
	binary.BigEndian.PutUint32(bu, m2M-4+1)

	un.OnData(bu[:4])

	assert.Equal(t, 4, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, len(*un.pBuffer), m4M)

	bu = un.GetRecvBuff()
	un.OnData(bu[:m2M-4+1])

	assert.Equal(t, m2M+1, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)

	assert.Equal(t, m2M-4+1, len(msg.([]byte)))

	////////

	bu = *un.pBuffer
	binary.BigEndian.PutUint32(bu, m4M-4+1)

	un.OnData(bu[:4])

	assert.Equal(t, 4, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, len(*un.pBuffer), m8M)

	bu = un.GetRecvBuff()
	un.OnData(bu[:m4M-4+1])

	assert.Equal(t, m4M+1, un.w)

	msg, err = un.Unpack()
	assert.Nil(t, err)

	assert.Equal(t, m4M-4+1, len(msg.([]byte)))

}
