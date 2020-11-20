package sqlnode

import (
	"github.com/jmoiron/sqlx"
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

			default:
				getLogger().Errorln("cmd-processor %d: invalid cmd type: %s.", p.no, reflect.TypeOf(list[i]))
				c = nil
			}

			if c != nil {
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

func (p *cmdProcessor) processCmd(cmd cmd, task sqlTask) sqlTask {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			n := runtime.Stack(buff, false)
			getLogger().Errorf("cmd-processor %d process cmd: %s.\n%s", p.no, err, buff[:n])
		}
	}()

	if cmd != nil && cmd.isProcessTimeout() {
		getLogger().Infof("cmd-processor %d: command %d is timeout, skip it.", p.no, cmd.seqNo())
		cmd = nil
		// todo something else ?
	}

	if task != nil {
		if cmd != nil && cmd.canCombine() && task.combine(cmd) {
			return task
		}

		task.do(p.db)
		task = nil
	}

	if cmd != nil {
		task = cmd.makeSqlTask()
		if !task.canCombine() {
			task.do(p.db)
			task = nil
		}
	}

	return task
}

var (
	globalCmdProcessors []*cmdProcessor
)

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

func pushCmd(c cmd) {
	globalCmdProcessors[util2.StringHash(c.uniKey())%len(globalCmdProcessors)].pushCmd(c)
}
