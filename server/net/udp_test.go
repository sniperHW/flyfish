package net

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestUdp(t *testing.T) {
	key := []byte("example key 1234")

	{

		packet := &sproto.PacketTest{
			Message: "hello world",
		}

		b, err := Pack(MakeMessage(100, packet))

		assert.Nil(t, err)

		m, err := Unpack(b)

		assert.Nil(t, err)

		msg := m.(*Message)

		assert.Equal(t, msg.Msg.(*sproto.PacketTest).Message, "hello world")

		assert.Equal(t, msg.Context, int64(100))

		//

		b, err = pack(key, MakeMessage(100, packet))

		assert.Nil(t, err)

		m, err = unpack(key, b)

		assert.Nil(t, err)

		msg = m.(*Message)

		assert.Equal(t, msg.Msg.(*sproto.PacketTest).Message, "hello world")

		assert.Equal(t, msg.Context, int64(100))

	}

	{

		packet := &sproto.PacketTest{
			Message: strings.Repeat("a", 4096),
		}

		b, err := Pack(MakeMessage(100, packet))

		assert.Nil(t, err)

		m, err := Unpack(b)

		assert.Nil(t, err)

		msg := m.(*Message)

		assert.Equal(t, msg.Msg.(*sproto.PacketTest).Message, strings.Repeat("a", 4096))

		assert.Equal(t, msg.Context, int64(100))

		//

		b, err = pack(key, MakeMessage(100, packet))

		assert.Nil(t, err)

		m, err = unpack(key, b)

		assert.Nil(t, err)

		msg = m.(*Message)

		assert.Equal(t, msg.Msg.(*sproto.PacketTest).Message, strings.Repeat("a", 4096))

		assert.Equal(t, msg.Context, int64(100))

	}

}
