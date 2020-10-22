package sqlnode

import (
	"github.com/jmoiron/sqlx"
	util2 "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet/util"
	"reflect"
)

type cmdProcessor struct {
	db       *sqlx.DB
	cmdQueue *util.BlockQueue
}

func newCmdProcessor(db *sqlx.DB) *cmdProcessor {
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
		closed, list := p.cmdQueue.Get()

		var n = len(list)
		var task sqlTask
		var combineNext = false
		var i = 0

		getLogger().Debugf("cmd-processor: start process %d commands.", n)

		for i < n {
			switch c := list[i].(type) {
			case *cmdGet:
				if c.isCancel() || c.isProcessTimeout() {
					getLogger().Infof("command %d is canceled or timeout, skip it.", c.seqNo())

					// todo something else ?
					break
				}

				if task == nil {
					task = c.makeSqlTask()
					combineNext = true
				} else {
					combineNext = task.combine(c)
				}

				if combineNext && i < n-1 {
					break
				}

				task.do(p.db)
				task.reply()
				task = nil

			default:
				getLogger().Errorln("invalid cmd type: %s.", reflect.TypeOf(list[i]))
			}

			i++
		}

		if closed {
			break
		}
	}
}

var (
	globalCmdProcessors []*cmdProcessor
)

func initCmdProcessor() {
	nProcessors := getConfig().DBConnections

	globalCmdProcessors = make([]*cmdProcessor, nProcessors)
	for i := 0; i < nProcessors; i++ {
		globalCmdProcessors[i] = newCmdProcessor(getGlobalDB())
		globalCmdProcessors[i].start()
	}
}

func pushCmd(c cmd) {
	globalCmdProcessors[util2.StringHash(c.uniKey())%len(globalCmdProcessors)].pushCmd(c)
}
