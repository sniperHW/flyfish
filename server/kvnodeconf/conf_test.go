package kvnodeconf

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test1(t *testing.T) {
	sslot.SlotCount = 128

	confJson := KvConfigJson{}

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          1,
		HostIP:      "10.127.0.1",
		RaftPort:    11,
		ServicePort: 21,
		ConsolePort: 31,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          2,
		HostIP:      "10.127.0.2",
		RaftPort:    12,
		ServicePort: 22,
		ConsolePort: 32,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          3,
		HostIP:      "10.127.0.3",
		RaftPort:    13,
		ServicePort: 23,
		ConsolePort: 33,
	})

	/////

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          4,
		HostIP:      "10.127.0.4",
		RaftPort:    14,
		ServicePort: 24,
		ConsolePort: 34,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          5,
		HostIP:      "10.127.0.5",
		RaftPort:    15,
		ServicePort: 25,
		ConsolePort: 35,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, Node{
		ID:          6,
		HostIP:      "10.127.0.6",
		RaftPort:    16,
		ServicePort: 26,
		ConsolePort: 36,
	})

	confJson.Shard = append(confJson.Shard, RaftGroupJson{
		Nodes: []int{1, 2, 3},
	})

	confJson.Shard = append(confJson.Shard, RaftGroupJson{
		Nodes: []int{4, 5, 6},
	})

	b, err := MarshalConfig(&confJson)
	assert.Nil(t, err)
	fmt.Println(string(b))

	conf, err := makeKvConfig(&confJson)
	assert.Nil(t, err)

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[0][0].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][0].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][0].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][0].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][0].Stores[4].Slots.GetOpenBits())

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[0][1].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][1].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][1].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][1].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][1].Stores[4].Slots.GetOpenBits())

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[0][2].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][2].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][2].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][2].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[0][2].Stores[4].Slots.GetOpenBits())

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[1][0].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][0].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][0].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][0].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][0].Stores[4].Slots.GetOpenBits())

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[1][1].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][1].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][1].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][1].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][1].Stores[4].Slots.GetOpenBits())

	fmt.Println("---------------------------------------------")

	fmt.Println(conf.Shard[1][2].Stores[0].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][2].Stores[1].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][2].Stores[2].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][2].Stores[3].Slots.GetOpenBits())
	fmt.Println(conf.Shard[1][2].Stores[4].Slots.GetOpenBits())

}
