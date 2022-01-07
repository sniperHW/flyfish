package flykv

//go test -covermode=count -v -coverprofile=../coverage.out -run=TestScaner
//go tool cover -html=../coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestScaner(t *testing.T) {
	slot.SlotCount = 128

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newSqlDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = i
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	fmt.Println("set ok")

	sc, _ := client.MakeScanner(client.ClientConf{SoloService: "localhost:10018"}, "users1", nil, true)

	count := 0

	for {
		rows, err := sc.Next(10, time.Now().Add(time.Second*5))
		if client.ErrScanFinish == err {
			break
		} else if nil != err {
			panic(err)
		}
		for _, v := range rows {
			fmt.Println(v.Key)
			count++
		}
	}

	assert.Equal(t, count, 100)

	node.Stop()

	//删除日志,的scan将从数据库直接返回

	os.RemoveAll("./testRaftLog")

	node = start1Node(newSqlDBBackEnd())

	sc, _ = client.MakeScanner(client.ClientConf{SoloService: "localhost:10018"}, "users1", nil, true)

	count = 0

	for {
		rows, err := sc.Next(10, time.Now().Add(time.Second*5))
		if client.ErrScanFinish == err {
			break
		} else if nil != err {
			panic(err)
		}

		//fmt.Println(len(rows))
		for _, v := range rows {
			fmt.Println(v.Key)
			count++
		}
	}

	assert.Equal(t, count, 100)

	node.Stop()

}
