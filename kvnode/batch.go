package kvnode

import (
	flyfish_logger "github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/util/fixedarray"
	"github.com/sniperHW/flyfish/util/str"
	"time"
)

var fixedArrayPool *fixedarray.FixedArrayPool = fixedarray.NewPool(100)
var replayOK *commitedBatchProposal = &commitedBatchProposal{}
var replaySnapshot *commitedBatchProposal = &commitedBatchProposal{}

//for linearizableRead

type readBatchSt struct {
	readIndex int64
	tasks     *fixedarray.FixedArray
	deadline  time.Time
}

type batchProposal struct {
	proposalStr *str.Str
	tasks       *fixedarray.FixedArray
	index       int64
}

type commitedBatchProposal struct {
	data  []byte
	tasks *fixedarray.FixedArray
}

func (this *readBatchSt) onError(err int32) {
	this.tasks.ForEach(func(v interface{}) {
		v.(asynCmdTaskI).onError(err)
	})
	fixedArrayPool.Put(this.tasks)
}

func (this *readBatchSt) reply() {
	this.tasks.ForEach(func(v interface{}) {
		v.(asynCmdTaskI).reply()
		v.(asynCmdTaskI).getKV().processCmd(nil)
	})
	fixedArrayPool.Put(this.tasks)
}

func (this *batchProposal) onError(err int32) {
	this.tasks.ForEach(func(v interface{}) {
		v.(asynTaskI).onError(err)
	})
	fixedArrayPool.Put(this.tasks)
	str.Put(this.proposalStr)
}

func (this *batchProposal) onPorposeTimeout() {
	this.tasks.ForEach(func(v interface{}) {
		v.(asynTaskI).onPorposeTimeout()
	})
}

func (this *commitedBatchProposal) apply(store *kvstore) {
	if nil != this.tasks {
		this.tasks.ForEach(func(v interface{}) {
			switch v.(type) {
			case asynCmdTaskI:
				v.(asynCmdTaskI).reply()
			}
		})

		this.tasks.ForEach(func(v interface{}) {
			v.(asynTaskI).done()
		})
	} else {
		if !store.apply(this.data, false) {
			flyfish_logger.GetSugar().Fatal("apply commitedBatchProposal failed")
		}
	}
}
