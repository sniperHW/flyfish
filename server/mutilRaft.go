package server

import (
	"github.com/sniperHW/flyfish/rafthttp"
	"github.com/xiang90/probing"
	"go.etcd.io/etcd/pkg/types"
	"net/http"
	"net/url"
	//"strconv"
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
	transports map[types.ID]*raftHandler
}

type mutilRaftHandler struct {
	mutilRaft      *mutilRaft
	probingHandler http.Handler
}

func (this *mutilRaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//Infoln(r.URL.Path)
	to := ""

	if tmp := strings.Split(r.URL.Path, "/"); len(tmp) >= 3 {
		cmd := tmp[2]
		if cmd == "probing" {
			if len(tmp) >= 4 {
				to = tmp[3]
			}
		} else {
			to = r.Header.Get("X-Raft-To")
		}

		if id, err := types.IDFromString(to); err == nil {
			if t := this.mutilRaft.getTransport(id); nil != t {
				if cmd == "stream" {
					t.streamHandler.ServeHTTP(w, r)
				} else if cmd == "snapshot" {
					t.snapHandler.ServeHTTP(w, r)
				} else if cmd == "probing" {
					this.probingHandler.ServeHTTP(w, r)
				}
			}
		}
	}
}

func (this *mutilRaft) addTransport(id types.ID, t *rafthttp.Transport) {
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

func (this *mutilRaft) getTransport(id types.ID) *raftHandler {
	this.RLock()
	defer this.RUnlock()
	return this.transports[id]
}

func (this *mutilRaft) removeTransport(id types.ID) {
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

	Infoln("serve", urlStr)

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
		transports: map[types.ID]*raftHandler{},
	}
}
