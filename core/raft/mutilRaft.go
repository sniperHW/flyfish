package raft

import (
	"errors"
	"github.com/sniperHW/flyfish/core/raft/rafthttp"
	"github.com/xiang90/probing"
	"go.etcd.io/etcd/pkg/types"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type raftHandler struct {
	pipelineHandler http.Handler
	streamHandler   http.Handler
	snapHandler     http.Handler
}

type MutilRaft struct {
	sync.RWMutex
	httpstopc  chan struct{}
	httpdonec  chan struct{}
	transports map[types.ID]*raftHandler
}

type mutilRaftHandler struct {
	mutilRaft      *MutilRaft
	probingHandler http.Handler
}

type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

func (this *mutilRaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	to := ""

	if tmp := strings.Split(r.URL.Path, "/"); len(tmp) >= 2 {
		cmd := ""

		if len(tmp) == 2 && tmp[1] == "raft" {
			cmd = "pipeline"
		} else if len(tmp) > 2 {
			cmd = tmp[2]
		} else {
			return
		}

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
				} else if cmd == "pipeline" {
					t.pipelineHandler.ServeHTTP(w, r)
				}
			}
		}
	}
}

func (this *MutilRaft) addTransport(id types.ID, t *rafthttp.Transport) {
	this.Lock()
	defer this.Unlock()

	GetSugar().Infof("addTransport %d", id)

	handler := &raftHandler{
		pipelineHandler: rafthttp.NewPipelineHandler(t, t.Raft, t.ClusterID),
		streamHandler:   rafthttp.NewStreamHandler(t, t, t.Raft, t.ID, t.ClusterID),
		snapHandler:     rafthttp.NewSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID),
	}

	this.transports[id] = handler
}

func (this *MutilRaft) getTransport(id types.ID) *raftHandler {
	this.RLock()
	defer this.RUnlock()
	return this.transports[id]
}

func (this *MutilRaft) removeTransport(id types.ID) {
	this.Lock()
	defer this.Unlock()
	delete(this.transports, id)
}

func (this *MutilRaft) Handler() http.Handler {
	return &mutilRaftHandler{
		mutilRaft:      this,
		probingHandler: probing.NewHandler(),
	}
}

func (this *MutilRaft) Serve(urlStr string) {
	url, err := url.Parse(urlStr)
	if err != nil {
		GetSugar().Fatalf("raftexample: Failed parsing URL %v", err)
	}

	var ln *stoppableListener

	for {
		ln, err = newStoppableListener(url.Host, this.httpstopc)
		if err != nil {
			GetSugar().Infof("raftexample: Failed to listen rafthttp %v", err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	GetSugar().Infof("serve:%s", urlStr)

	err = (&http.Server{Handler: this.Handler()}).Serve(ln)

	select {
	case <-this.httpstopc:
	default:
		GetSugar().Fatalf("raftexample: Failed to serve rafthttp %v", err)
	}

	close(this.httpdonec)
}

func (this *MutilRaft) Stop() {
	close(this.httpstopc)
	<-this.httpdonec
}

func NewMutilRaft() *MutilRaft {
	return &MutilRaft{
		httpstopc:  make(chan struct{}),
		httpdonec:  make(chan struct{}),
		transports: map[types.ID]*raftHandler{},
	}
}
