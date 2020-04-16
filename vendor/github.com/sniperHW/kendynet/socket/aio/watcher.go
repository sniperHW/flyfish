// +build darwin netbsd freebsd openbsd dragonfly linux

package aio

import (
	"github.com/sniperHW/aiogo"
	"math/rand"
	"runtime"
)

type AioService struct {
	watchers            []*aiogo.Watcher
	readCompleteQueues  []*aiogo.CompleteQueue
	writeCompleteQueues []*aiogo.CompleteQueue
}

func completeRoutine(completeQueue *aiogo.CompleteQueue) {
	for {
		es, ok := completeQueue.Get()
		if !ok {
			return
		} else {
			for ; nil != es; es = es.Next() {
				c := es.Ud.(*AioSocket)
				if es.Type == aiogo.Read {
					c.onRecvComplete(es)
				} else {
					c.onSendComplete(es)
				}
			}
		}
	}
}

func NewAioService(watcherCount int, completeQueueCount int, workerCount int, buffPool aiogo.BufferPool) *AioService {

	s := &AioService{
		watchers:            []*aiogo.Watcher{},
		readCompleteQueues:  []*aiogo.CompleteQueue{},
		writeCompleteQueues: []*aiogo.CompleteQueue{},
	}

	if watcherCount <= 0 {
		watcherCount = 1
	}

	if completeQueueCount <= 0 {
		completeQueueCount = runtime.NumCPU() * 2
	}

	for i := 0; i < watcherCount; i++ {
		watcher, err := aiogo.NewWatcher(&aiogo.WatcherOption{
			BufferPool:  buffPool,
			WorkerCount: workerCount,
		})
		if nil != err {
			return nil
		}
		s.watchers = append(s.watchers, watcher)
	}

	for i := 0; i < completeQueueCount; i++ {
		queue := aiogo.NewCompleteQueue()
		s.readCompleteQueues = append(s.readCompleteQueues, queue)
		go completeRoutine(queue)
	}

	for i := 0; i < completeQueueCount; i++ {
		queue := aiogo.NewCompleteQueue()
		s.writeCompleteQueues = append(s.writeCompleteQueues, queue)
		go completeRoutine(queue)
	}

	return s
}

func (this *AioService) getWatcherAndCompleteQueue() (*aiogo.Watcher, *aiogo.CompleteQueue, *aiogo.CompleteQueue) {
	r := rand.Int()
	return this.watchers[r%len(this.watchers)], this.readCompleteQueues[r%len(this.readCompleteQueues)], this.writeCompleteQueues[r%len(this.writeCompleteQueues)]
}
