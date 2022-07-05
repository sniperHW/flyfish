package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/client/test/config"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/slot"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type statistics struct {
	sync.Mutex
	v   []time.Duration
	t   map[string]int
	err int
}

func (s *statistics) add(t string, d time.Duration, err errcode.Error) {
	s.Lock()
	s.v = append(s.v, d)
	if c, ok := s.t[t]; ok {
		s.t[t] = c + 1
	} else {
		s.t[t] = 1
	}
	if nil != err {
		s.err++
	}
	s.Unlock()
}

func (s *statistics) reset() (time.Duration, int, map[string]int, int) {
	s.Lock()
	v := s.v
	s.v = []time.Duration{}
	t := s.t
	s.t = map[string]int{}
	err := s.err
	s.err = 0
	s.Unlock()

	if len(v) > 0 {
		var d time.Duration
		for _, vv := range v {
			d += vv
		}
		return d / time.Duration(len(v)), len(v), t, err
	} else {
		return 0, 0, t, err
	}
}

var st statistics

var phones [][]byte

func init() {
	size := []int{
		32,
		64,
		128,
		256,
		512,
		1024,
		2048,
		4096,
		1024 * 16,
	}

	for i := 0; i < len(size); i++ {
		b := make([]byte, size[int(rand.Int31())%len(size)])
		for j, _ := range b {
			b[j] = byte(rand.Int31() % 127)
		}
		phones = append(phones, b)
	}
}

func randomPhone() []byte {
	return phones[int(rand.Int31())%len(phones)]
}

func init() {
	v := [][]interface{}{
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//set
				fields := map[string]interface{}{}
				fields["age"] = rand.Int31() % 100
				fields["phone"] = randomPhone()
				fields["name"] = unikey
				beg := time.Now()
				r := c.Set("users1", unikey, fields).Exec()
				st.add("set", time.Now().Sub(beg), r.ErrCode)
			}, 100},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//kick
				beg := time.Now()
				r := c.Kick("users1", unikey).Exec()
				st.add("kick", time.Now().Sub(beg), r.ErrCode)
			}, 1},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//inc
				beg := time.Now()
				r := c.IncrBy("users1", unikey, "age", int64(rand.Int31()%100)).Exec()
				st.add("inc", time.Now().Sub(beg), r.ErrCode)
			}, 100},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//dec
				beg := time.Now()
				r := c.DecrBy("users1", unikey, "age", int64(rand.Int31()%100)).Exec()
				st.add("dec", time.Now().Sub(beg), r.ErrCode)
			}, 100},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//del
				beg := time.Now()
				r := c.Del("users1", unikey).Exec()
				st.add("del", time.Now().Sub(beg), r.ErrCode)
			}, 50},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//cmpset
				beg := time.Now()
				r := c.Get("users1", unikey, "age").Exec()
				if nil == r.ErrCode {
					st.add("get", time.Now().Sub(beg), nil)
					beg = time.Now()
					r := c.CompareAndSet("users1", unikey, "age", r.Fields["age"].GetInt(), r.Fields["age"].GetInt()+1).Exec()
					st.add("cmpset", time.Now().Sub(beg), r.ErrCode)
				} else if errcode.GetCode(r.ErrCode) == errcode.Errcode_record_notexist {
					st.add("get", time.Now().Sub(beg), nil)
					beg = time.Now()
					r := c.CompareAndSetNx("users1", unikey, "age", 10, 20).Exec()
					st.add("cmpset", time.Now().Sub(beg), r.ErrCode)
				} else {
					st.add("get", time.Now().Sub(beg), r.ErrCode)
				}
			}, 100},
		[]interface{}{
			func(c *kclient.Client, unikey string) {
				//get
				beg := time.Now()
				r := c.GetAll("users1", unikey).Exec()
				st.add("get", time.Now().Sub(beg), r.ErrCode)
			}, 100},
	}

	for _, vv := range v {
		for i := 0; i < vv[1].(int); i++ {
			cmds = append(cmds, vv[0].(func(*kclient.Client, string)))
		}
	}
}

var cmds []func(*kclient.Client, string)

func getCmd() func(*kclient.Client, string) {
	return cmds[int(rand.Int31())%len(cmds)]
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("bin keyrange")
		return
	}

	cfg, err := config.LoadConfig("./config.toml")

	if nil != err {
		panic(err)
	}

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, 10, true))

	clientCfg := kclient.ClientConf{
		ClientType: kclient.ClientType_FlyKv,
	}

	if cfg.Mode == "solo" {
		clientCfg.SoloConf = &kclient.SoloConf{
			Service:         cfg.Service,
			UnikeyPlacement: slot.MakeUnikeyPlacement(cfg.Stores),
		}
	} else {
		clientCfg.ClusterConf = &kclient.ClusterConf{
			PD: strings.Split(cfg.PD, ";"),
		}
	}

	keyrange, _ := strconv.ParseInt(os.Args[1], 10, 32)

	st.reset()

	for j := 0; j < 10; j++ {
		c, _ := kclient.OpenClient(clientCfg)
		for i := 0; i < 500; i++ {
			go func() {
				for {
					unikey := fmt.Sprintf("sniperHW:%d", int(rand.Int31())%int(keyrange))
					cmd := getCmd()
					cmd(c, unikey)
				}
			}()
		}
	}

	go func() {
		for {
			time.Sleep(time.Second)

			d, c, t, e := st.reset()

			fmt.Println(d, c, e, t)
		}
	}()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
