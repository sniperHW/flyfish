package main

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/kvnode"
)

type result struct {
	latency time.Duration
	err     errcode.Error
}

type ByTime []result

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].latency < a[j].latency }

var (
	id       int64
	total    int64
	sigStop  chan bool = make(chan bool)
	results  []result  = []result{}
	mtx      sync.Mutex
	keyrange int64
	bar      *progressbar.ProgressBar
)

func Set(c *kclient.Client) bool {

	nextID := atomic.AddInt64(&id, 1)
	if nextID > total {
		return false
	}

	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	//fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int63()%keyrange)
	set := c.Set("users1", key, fields)

	beg := time.Now()

	set.AsyncExec(func(ret *kclient.StatusResult) {

		latency := time.Now().Sub(beg)
		r := result{
			latency: latency,
			err:     ret.ErrCode,
		}
		var n int
		mtx.Lock()
		results = append(results, r)
		n = len(results)
		mtx.Unlock()
		if int64(n) == total {
			sigStop <- true
		}
		bar.Add(1)
		//Set(c)
	})
	return true
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("bin keyrange count ip:port")
		return
	}

	rand.Seed(int64(time.Now().Unix()))

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)
	total, _ = strconv.ParseInt(os.Args[2], 10, 32)

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, true))

	id = 0

	bar = progressbar.New(int(total))

	for j := 0; j < 100; j++ {
		c := kclient.OpenClient(os.Args[3]).SetUnikeyPlacement(kvnode.MakeUnikeyPlacement([]int{1, 2, 3, 4, 5}))
		go func() {
			for {
				for i := 0; i < 50; i++ {
					if !Set(c) {
						return
					}
				}
				time.Sleep(time.Millisecond * 50)
			}
		}()
	}

	sigStop = make(chan bool)
	_, _ = <-sigStop

	mtx.Lock()

	success := 0
	busy := 0
	timeout := 0
	otherErr := 0
	avagelatency := time.Duration(0)

	for _, v := range results {
		if v.err == nil {
			success++
		} else if v.err.Code == errcode.Errcode_retry {
			busy++
		} else if v.err.Code == errcode.Errcode_timeout {
			timeout++
		} else {
			otherErr++
		}

		avagelatency += v.latency

	}

	fmt.Print("\n")

	fmt.Println("total:", total, "success:", success, "busy:", busy, "timeout:", timeout, "otherErr:", otherErr)

	sort.Sort(ByTime(results))

	fmt.Println("avagelatency:", avagelatency/time.Duration(total), "min:", results[0].latency, "max:", results[total-1].latency)

	p99 := int(float64(total) * 0.99)
	fmt.Println("p99:", results[p99-1].latency)

	p90 := int(float64(total) * 0.90)
	fmt.Println("p90:", results[p90-1].latency)

	p80 := int(float64(total) * 0.80)
	fmt.Println("p80:", results[p80-1].latency)

	p70 := int(float64(total) * 0.70)
	fmt.Println("p70:", results[p70-1].latency)

	p60 := int(float64(total) * 0.60)
	fmt.Println("p60:", results[p60-1].latency)

	p50 := int(float64(total) * 0.50)
	fmt.Println("p50:", results[p50-1].latency)

	mtx.Unlock()
}
