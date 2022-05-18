package flypd

import (
	//"crypto/tls"
	"fmt"
	"github.com/gogo/protobuf/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	//"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type httpReplyer struct {
	replyed int32
	waitCh  chan struct{}
	w       http.ResponseWriter
}

func (h *httpReplyer) reply(resp *snet.Message) {
	if atomic.CompareAndSwapInt32(&h.replyed, 0, 1) {
		if byte, err := proto.Marshal(resp.Msg); nil == err {
			h.w.Write(byte)
		}
		close(h.waitCh)
	}
}

func (p *pd) fetchReq(cmd string, r *http.Request) (*snet.Message, error) {
	if h, ok := p.msgHandler.makeHttpReq[cmd]; ok {
		return h(r)
	} else {
		return nil, fmt.Errorf("unknown cmd:%s", cmd)
	}
}

func (p *pd) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if p.config.TLS.EnableTLS {
		if r.Header.Get("token") != "feiyu_tech_2021" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if atomic.LoadInt32(&p.closed) == 1 || !p.isLeader() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if tmp := strings.Split(r.URL.Path, "/"); len(tmp) == 2 {
		if tmp[1] == "QueryPdLeader" {
			if byte, err := proto.Marshal(&sproto.QueryPdLeaderResp{
				Yes: p.isLeader(),
			}); nil == err {
				w.Write(byte)
			}
		} else {
			req, err := p.fetchReq(tmp[1], r)
			if nil == err {
				replyer := &httpReplyer{w: w, waitCh: make(chan struct{})}
				err = p.mainque.append(func() {
					if p.isLeader() {
						p.onMsg(replyer, req)
					} else {
						w.WriteHeader(http.StatusServiceUnavailable)
						close(replyer.waitCh)
					}
				})

				if nil == err {
					ticker := time.NewTicker(time.Second * 5)
					select {
					case <-replyer.waitCh:
					case <-ticker.C:
						w.WriteHeader(http.StatusRequestTimeout)
					}
					ticker.Stop()
				} else {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			} else {
				GetSugar().Errorf("ServeHTTP error:%v", err)
				w.WriteHeader(http.StatusBadRequest)
			}
		}
	}
}

func (p *pd) startHttpService() {
	p.httpServer = &http.Server{
		Addr:           p.service,
		Handler:        p,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	GetSugar().Infof("pd start http on:%v", p.service)

	if p.config.TLS.EnableTLS {
		go func() {
			if err := p.httpServer.ListenAndServeTLS(p.config.TLS.Crt, p.config.TLS.Key); nil != err && err != http.ErrServerClosed {
				GetSugar().Errorf("ListenAndServeTLS at %s err:%v", p.service, err)
			}
		}()

	} else {
		go func() {
			if err := p.httpServer.ListenAndServe(); nil != err && err != http.ErrServerClosed {
				GetSugar().Errorf("ListenAndServe at %s err:%v", p.service, err)
			}
		}()
	}
}
