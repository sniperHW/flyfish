package server

import (
	"github.com/sniperHW/flyfish/rafthttp"
	"github.com/xiang90/probing"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type raftHandler struct {
	pipelineHandler http.Handler
	streamHandler   http.Handler
	snapHandler     http.Handler
}

type mutilRaft struct {
	sync.RWMutex
	httpstopc  chan struct{}
	httpdonec  chan struct{}
	transports map[int]*raftHandler
}

type mutilRaftHandler struct {
	mutilRaft      *mutilRaft
	probingHandler http.Handler
}

func (this *mutilRaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tmp := strings.Split(r.URL.Path, "/")
	Infoln(r.URL.Path)
	if len(tmp) >= 3 {
		if tmp[2] == "raft" {
			id, err := strconv.ParseInt(tmp[1], 10, 64)
			Infoln(id)
			if nil == err {
				t := this.mutilRaft.getTransport(int(id))
				if nil != t {
					if len(tmp) > 3 {
						if tmp[3] == "stream" {
							t.streamHandler.ServeHTTP(w, r)
						} else if tmp[3] == "snapshot" {
							t.snapHandler.ServeHTTP(w, r)
						} else if tmp[3] == "probing" {
							this.probingHandler.ServeHTTP(w, r)
						}
					} else {
						t.pipelineHandler.ServeHTTP(w, r)
					}
				} else {
					panic(id)
				}
			}
		}
	}
}

func (this *mutilRaft) addTransport(id int, t *rafthttp.Transport) {
	this.Lock()
	defer this.Unlock()

	Infoln("addTransport", id)

	handler := &raftHandler{
		pipelineHandler: rafthttp.NewPipelineHandler(t, t.Raft, t.ClusterID),
		streamHandler:   rafthttp.NewStreamHandler(t, t, t.Raft, t.ID, t.ClusterID),
		snapHandler:     rafthttp.NewSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID),
	}

	this.transports[id] = handler
}

func (this *mutilRaft) getTransport(id int) *raftHandler {
	this.RLock()
	defer this.RUnlock()
	return this.transports[id]
}

func (this *mutilRaft) removeTransport(id int) {
	this.Lock()
	defer this.Unlock()
	delete(this.transports, id)
}

func (this *mutilRaft) Handler() http.Handler {
	return &mutilRaftHandler{
		mutilRaft:      this,
		probingHandler: probing.NewHandler(),
	}
}

func (this *mutilRaft) serveMutilRaft(urlStr string) {
	url, err := url.Parse(urlStr)
	if err != nil {
		Fatalln("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, this.httpstopc)
	if err != nil {
		Fatalln("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: this.Handler()}).Serve(ln)

	select {
	case <-this.httpstopc:
	default:
		Fatalln("raftexample: Failed to serve rafthttp (%v)", err)
	}

	close(this.httpdonec)
}

func newMutilRaft() *mutilRaft {
	return &mutilRaft{
		httpstopc:  make(chan struct{}),
		httpdonec:  make(chan struct{}),
		transports: map[int]*raftHandler{},
	}
}
