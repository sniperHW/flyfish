package flykv

//go test -race -covermode=atomic -v -coverprofile=../coverage.out -run=TestFriends
//go tool cover -html=../coverage.out

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/queue"
	flykv "github.com/sniperHW/flyfish/server/flykv"
	flypd "github.com/sniperHW/flyfish/server/flypd"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

type dbconf struct {
	DBType string
	Usr    string
	Pwd    string
	DB     string
	Host   string
	Port   int
}

var flyKvConfigStr string = `

PD                        = "localhost:8110"

SnapshotCurrentCount      = 0

SnapshotCount             = 10000
SnapshotCatchUpEntriesN   = 5000

MainQueueMaxSize          = 10000

MaxCachePerStore          = 10000                #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize       = 200                  #sql加载管道线大小

SqlLoadQueueSize          = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount            = 5
SqlUpdaterCount           = 20

ProposalFlushInterval     = 100
ReadFlushInterval         = 10 

RaftLogDir                = "testRaftLog/flykv"

RaftLogPrefix             = "flykv"

LinearizableRead          = false

WriteBackMode             = "WriteThrough"

[DBConfig]
DBType        = "%s"
Host          = "%s"
Port          = %d
User	      = "%s"
Password      = "%s"
DB            = "%s"

[StoreReqLimit]
SoftLimit               = 50000
HardLimit               = 100000
SoftLimitSeconds        = 10

`

var pdConfigStr string = `
	MainQueueMaxSize = 1000
	RaftLogDir              = "testRaftLog"
	RaftLogPrefix           = "flypd"
	InitDepoymentPath       = "./deployment.json"
	InitMetaPath            = "./meta.json"
`

var dbConf *dbconf

type StopAble interface {
	Stop()
}

func init() {
	l := logger.NewZapLogger("integrationTest.log", "./log", "Debug", 104857600, 14, 10, true)
	flypd.InitLogger(l)
	flykv.InitLogger(l)
	client.InitLogger(l)
	logger.InitLogger(l)
	dbConf = &dbconf{}
	if _, err := toml.DecodeFile("../test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}
}

func newPD() StopAble {
	conf, _ := flypd.LoadConfigStr(pdConfigStr)

	conf.DBConfig.DBType = dbConf.DBType
	conf.DBConfig.Host = dbConf.Host
	conf.DBConfig.Port = dbConf.Port
	conf.DBConfig.User = dbConf.Usr
	conf.DBConfig.Password = dbConf.Pwd
	conf.DBConfig.DB = dbConf.DB

	raftID := idutil.NewGenerator(0, time.Now()).Next()

	raftCluster := fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@voter", raftID)

	pd, _ := flypd.NewPd(1, 1, false, conf, raftCluster)

	return pd
}

type objectFriends struct {
	id      int
	Friends map[int]bool
	queue   *list.List
	lock    bool
	mgr     *relationsMgr
}

func (r *objectFriends) unlock() {
	r.lock = false
	if front := r.queue.Front(); nil != front {
		op := front.Value.(*opration)
		if !op.o1.lock && !op.o2.lock {
			op.o1.queue.Remove(op.listEle1)
			op.o2.queue.Remove(op.listEle2)
			op.o1.lock = true
			op.o2.lock = true
			//将op投入执行
			r.mgr.doOperation(op.do)
		}
	}
}

type opration struct {
	o1            *objectFriends
	o2            *objectFriends
	listEle1      *list.Element
	listEle2      *list.Element
	flyfishClient *client.Client
	wait          *sync.WaitGroup
}

func (o *opration) do() {

	//fmt.Println("do", o.o1.id, o.o2.id)

	r1 := o.flyfishClient.Get("friends", fmt.Sprintf("%d", o.o1.id), "friends").Exec()
	r2 := o.flyfishClient.Get("friends", fmt.Sprintf("%d", o.o2.id), "friends").Exec()

	f1 := map[int]bool{}
	f2 := map[int]bool{}

	if r1.ErrCode == nil {
		json.Unmarshal(r1.Fields["friends"].GetBlob(), &f1)
	}

	if r2.ErrCode == nil {
		json.Unmarshal(r2.Fields["friends"].GetBlob(), &f2)
	}

	f1[o.o2.id] = true
	f2[o.o1.id] = true

	b1, _ := json.Marshal(f1)
	b2, _ := json.Marshal(f2)

	o.flyfishClient.Set("friends", fmt.Sprintf("%d", o.o1.id), map[string]interface{}{
		"friends": b1,
	}).Exec()

	o.flyfishClient.Set("friends", fmt.Sprintf("%d", o.o2.id), map[string]interface{}{
		"friends": b2,
	}).Exec()

	o.o1.Friends = f1
	o.o2.Friends = f2

	o.o1.mgr.pushFunc(o.release)
}

func (o *opration) release() {
	o.o1.unlock()
	o.o2.unlock()
	o.wait.Done()
}

type relationsMgr struct {
	objects map[int]*objectFriends
	q       *queue.PriorityQueue
	ch      chan func()
}

func (rm *relationsMgr) addFriend(a int, b int, c *client.Client, wait *sync.WaitGroup) {
	oA := rm.objects[a]
	if nil == oA {
		oA = &objectFriends{
			id:    a,
			queue: list.New(),
			mgr:   rm,
		}
		rm.objects[a] = oA
	}

	oB := rm.objects[b]
	if nil == oB {
		oB = &objectFriends{
			id:    b,
			queue: list.New(),
			mgr:   rm,
		}
		rm.objects[b] = oB
	}

	op := &opration{
		o1:            oA,
		o2:            oB,
		flyfishClient: c,
		wait:          wait,
	}

	if oA.lock || oB.lock {
		//有任意一个对象被锁定，将op添加到两个对象的队列中
		op.listEle1 = oA.queue.PushBack(op)
		op.listEle2 = oB.queue.PushBack(op)
	} else {
		oA.lock = true
		oB.lock = true
		rm.doOperation(op.do)
	}
}

func (rm *relationsMgr) stop() {
	for len(rm.ch) > 0 {
		runtime.Gosched()
	}
	close(rm.ch)
	rm.q.Close()
}

func (rm *relationsMgr) startWorker() {
	_ = runtime.NumCPU()
	for i := 0; i < 1000; i++ {
		go func() {
			for o := range rm.ch {
				o()
			}
		}()
	}
}

func (rm *relationsMgr) pushFunc(f func()) {
	rm.q.ForceAppend(0, f)
}

func (rm *relationsMgr) doOperation(o func()) {
	rm.ch <- o
}

func TestFriends(t *testing.T) {
	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	kvConf, err := flykv.LoadConfigStr(fmt.Sprintf(flyKvConfigStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Usr, dbConf.Pwd, dbConf.DB))

	if nil != err {
		panic(err)
	}

	pd := newPD()

	node, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	c, _ := client.New(client.ClientConf{
		ClientType: client.FlyKv,
		PD:         []string{"localhost:8110"},
	})

	for {
		r := c.GetAll("friends", "1").Exec()
		if nil != r.ErrCode && r.ErrCode.Code == errcode.Errcode_record_notexist {
			break
		}
	}

	var objects1 []int
	var objects2 []int

	{

		rm := &relationsMgr{
			objects: map[int]*objectFriends{},
			q:       queue.NewPriorityQueue(1, 10000),
			ch:      make(chan func(), 10000),
		}

		rm.startWorker()

		var wait sync.WaitGroup

		beg := time.Now()

		wait.Add(5000)

		go func() {
			//添加1000个任务
			for i := 0; i < 5000; i++ {
				a := int(rand.Int31() % 5000)
				b := int(rand.Int31() % 5000)
				if a == b {
					b += 5000
				}

				objects1 = append(objects1, a)
				objects2 = append(objects2, b)

				rm.addFriend(a, b, c, &wait)
			}

			for {
				closed, v := rm.q.Pop()
				if closed {
					break
				} else {
					v.(func())()
				}
			}

		}()

		wait.Wait()

		logger.GetSugar().Infof("use:%v", time.Now().Sub(beg))

		rm.stop()

	}

	logger.GetSugar().Infof("stop")

	node.Stop()

	c.Close()

	node, err = flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	c, _ = client.New(client.ClientConf{
		ClientType: client.FlyKv,
		PD:         []string{"localhost:8110"},
	})

	for {
		r := c.GetAll("friends", "1").Exec()
		if nil == r.ErrCode || r.ErrCode.Code == errcode.Errcode_record_notexist {
			break
		}
	}

	//again,此时flykv已经将数据加载进缓存中，无需再从数据库导入

	{

		rm := &relationsMgr{
			objects: map[int]*objectFriends{},
			q:       queue.NewPriorityQueue(1, 10000),
			ch:      make(chan func(), 10000),
		}

		rm.startWorker()

		var wait sync.WaitGroup

		beg := time.Now()

		wait.Add(5000)

		go func() {
			//添加1000个任务
			for i := 0; i < 5000; i++ {
				a := objects1[i]
				b := objects2[i]
				rm.addFriend(a, b, c, &wait)
			}

			for {
				closed, v := rm.q.Pop()
				if closed {
					break
				} else {
					v.(func())()
				}
			}

		}()

		wait.Wait()

		logger.GetSugar().Infof("use:%v", time.Now().Sub(beg))

		rm.stop()

	}

	logger.GetSugar().Infof("stop")

	node.Stop()

	c.Close()

	pd.Stop()

}
