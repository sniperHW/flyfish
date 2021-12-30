package main

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
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

func setMeta() {

	m := &db.DbDef{
		TableDefs: []*db.TableDef{
			&db.TableDef{
				Name: "users1",
				Fields: []*db.FieldDef{
					&db.FieldDef{
						Name:        "name",
						Type:        "string",
						DefautValue: "",
					},
					&db.FieldDef{
						Name:        "age",
						Type:        "int",
						DefautValue: "",
					},
					&db.FieldDef{
						Name:        "phone",
						Type:        "string",
						DefautValue: "",
					},
				},
			},
		},
	}

	b, _ := db.DbDefToJsonString(m)

	conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.SetMeta{
		Meta: b,
	}))

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
	fmt.Println("setMeta", r)
}

func main() {
	setMeta()
	installDeployment()
}
