package sqlnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	util2 "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type cmdProcessor struct {
	state    int32
	no       int
	db       *sqlx.DB
	cmdQueue *util.BlockQueue
}

func newCmdProcessor(no int, db *sqlx.DB) *cmdProcessor {
	return &cmdProcessor{
		no:       no,
		db:       db,
		cmdQueue: util.NewBlockQueue(),
	}
}

func (p *cmdProcessor) start() {
	if !atomic.CompareAndSwapInt32(&p.state, 0, 1) {
		panic("already started")
	}

	go p.process()
}

func (p *cmdProcessor) stop(wg *sync.WaitGroup) {
	if !atomic.CompareAndSwapInt32(&p.state, 1, 2) {
		panic("not started or already stopped")
	}

	p.cmdQueue.Close()

	if wg != nil {
		wg.Add(1)

		go func() {
			for atomic.LoadInt32(&p.state) != 3 {
				time.Sleep(100 * time.Millisecond)
			}

			getLogger().Infof("cmd-processor %d: stop.", p.no)

			wg.Done()
		}()
	}
}

func (p *cmdProcessor) pushCmd(c cmd) {
	p.cmdQueue.AddNoWait(c)
}

func (p *cmdProcessor) process() {
	for atomic.LoadInt32(&p.state) != 2 {
		var (
			closed bool
			list   []interface{}
			n      int
			i      = 0
			c      cmd
			task   sqlTask
		)

		closed, list = p.cmdQueue.Get()
		n = len(list)

		getLogger().Debugf("cmd-processor %d: start process %d commands.", p.no, n)

		for i < n && atomic.LoadInt32(&p.state) != 2 {
			switch o := list[i].(type) {
			case cmd:
				c = o
				if c.isProcessTimeout() {
					// 超时
					getLogger().Infof("cmd-processor %d: command %d is timeout, skip it.", p.no, c.seqNo())
					completeCmd(c)
					c = nil
				}

			default:
				getLogger().Errorln("cmd-processor %d: invalid cmd type: %s.", p.no, reflect.TypeOf(list[i]))
				c = nil
			}

			if c != nil || task != nil {
				task = p.processCmd(c, task)
			}

			i++
		}

		if task != nil {
			p.processCmd(nil, task)
		}

		if closed {
			break
		}
	}

	getLogger().Debugf("cmd-processor %d: stop process.", p.no)
	atomic.StoreInt32(&p.state, 3)
}

func (p *cmdProcessor) processCmd(cmd cmd, t sqlTask) (task sqlTask) {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			n := runtime.Stack(buff, false)
			getLogger().Fatalf("cmd-processor %d process cmd: %s.\n%s", p.no, err, buff[:n])
			//getLogger().Fatalf("cmd-processor %d process cmd: %s.\n", p.no, err)
			task = nil
		}
	}()

	task = t

	if task != nil {
		if cmd != nil && cmd.canCombine() && task.combine(cmd) {
			return task
		}

		doTask(task, p.db)
		task = nil
	}

	if cmd != nil {
		task = cmd.makeSqlTask()
		if !task.canCombine() {
			doTask(task, p.db)
			task = nil
		}
	}

	return task
}

var globalCmdProcessors []*cmdProcessor

func initCmdProcessor() {
	nProcessors := getConfig().DBConnections

	globalCmdProcessors = make([]*cmdProcessor, nProcessors)
	for i := 0; i < nProcessors; i++ {
		globalCmdProcessors[i] = newCmdProcessor(i, getGlobalDB())
		globalCmdProcessors[i].start()
	}

	getLogger().Infof("init processor: count=%d.", nProcessors)
}

func stopCmdProcessor() {
	var wg sync.WaitGroup

	for _, v := range globalCmdProcessors {
		v.stop(&wg)
	}

	wg.Wait()

	getLogger().Infof("processor stop.")
}

var commandCount int32

func processCmd(c cmd) {
	if atomic.LoadInt32(&commandCount) >= int32(getConfig().MaxRequestCount) {
		c.replyError(errcode.ERR_RETRY)
		return
	}

	atomic.AddInt32(&commandCount, 1)
	globalCmdProcessors[util2.StringHash(c.uniKey())%len(globalCmdProcessors)].pushCmd(c)
}

func completeCmd(c cmd) {
	n := atomic.AddInt32(&commandCount, -1)
	if n == 0 {
		getLogger().Infof("command count 0")
	}
}

func doTask(t sqlTask, db *sqlx.DB) {
	t.do(db)

	//
	for _, v := range t.getCommands() {
		completeCmd(v)
	}
}
