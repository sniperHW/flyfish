package net

//go test -run=.

import (
	"fmt"
	proto "github.com/gogo/protobuf/proto"
	pdproto "github.com/sniperHW/flyfish/server/pd/proto"
	"net"
	"testing"
	"time"
)

func TestUdp(t *testing.T) {
	serverAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:8110")
	clientAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:8111")

	go func() {
		server, err := NewUdp("127.0.0.1:8110")
		if nil != err {
			panic(err)
		}

		buff := make([]byte, 4096)

		_, msg, err := server.ReadFrom(buff)

		if nil != err {
			panic(err)
		}

		fmt.Println(msg)

		err = server.SendTo(clientAddr, &pdproto.KvnodeLoginResp{
			Ok: proto.Bool(true),
		})

		if nil != err {
			panic(err)
		}

		time.Sleep(time.Second)

	}()

	time.Sleep(time.Second)

	client, err := NewUdp("127.0.0.1:8111")
	if nil != err {
		panic(err)
	}

	err = client.SendTo(serverAddr, &pdproto.KvnodeLogin{
		NodeID: proto.Int32(1),
	})

	if nil != err {
		panic(err)
	}

	buff := make([]byte, 4096)

	_, msg, err := client.ReadFrom(buff)

	if nil != err {
		panic(err)
	}

	fmt.Println(msg)

}
