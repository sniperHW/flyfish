package sqlnode

import (
	"github.com/jmoiron/sqlx"
	util2 "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"runtime"
)

type cmdProcessor struct {
	no       int
	db       *sqlx.DB
	cmdQueue *util.BlockQueue
}

func newCmdProcessor(no int, db *sqlx.DB) *cmdProcessor {
	return &cmdProcessor{
		db:       db,
		cmdQueue: util.NewBlockQueue(),
	}
}

func (p *cmdProcessor) start() {
	go p.process()
}

func (p *cmdProcessor) pushCmd(c cmd) {
	p.cmdQueue.AddNoWait(c)
}

func (p *cmdProcessor) process() {
	for {
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

		for i < n {
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
		if cmd != nil && task.combine(cmd) {
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
}

func pushCmd(c cmd) {
	globalCmdProcessors[util2.StringHash(c.uniKey())%len(globalCmdProcessors)].pushCmd(c)
}
