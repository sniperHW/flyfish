package main

import (
	"fmt"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

func installDeployment() {

	conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	install := &sproto.InstallDeployment{}
	set1 := &sproto.DeploymentSet{SetID: 1}
	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "localhost",
		ServicePort: 9110,
		RaftPort:    9111,
	})
	install.Sets = append(install.Sets, set1)

	conn.SendTo(addr, install)

	ch := make(chan interface{})

	go func() {
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		if nil == err {
			select {
			case ch <- r:
			default:
			}
		}
	}()

	ticker := time.NewTicker(3 * time.Second)

	var r interface{}

	select {
	case r = <-ch:
	case <-ticker.C:
	}
	ticker.Stop()

	conn.Close()

	fmt.Println("installDeployment", r)
}

func main() {
	installDeployment()
}
