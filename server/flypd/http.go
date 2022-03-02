package flypd

import (
	"crypto/tls"
	"fmt"
	"github.com/gogo/protobuf/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
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

	if !p.isLeader() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if tmp := strings.Split(r.URL.Path, "/"); len(tmp) == 2 {
		GetSugar().Infof("http request %v", tmp[1])

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
				p.mainque.append(func() {
					if p.isLeader() {
						p.onMsg(replyer, req)
					} else {
						w.WriteHeader(http.StatusServiceUnavailable)
						close(replyer.waitCh)
					}
				})

				ticker := time.NewTicker(time.Second * 5)
				select {
				case <-replyer.waitCh:
				case <-ticker.C:
					w.WriteHeader(http.StatusRequestTimeout)
				}
				ticker.Stop()
			} else {
				GetSugar().Errorf("ServeHTTP error:%v", err)
				w.WriteHeader(http.StatusBadRequest)
			}
		}
	}
}

func (p *pd) startHttpService() error {
	p.httpServer = &http.Server{
		Handler:        p,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	var err error

	if p.config.TLS.EnableTLS {

		cert, err := tls.LoadX509KeyPair(p.config.TLS.Crt, p.config.TLS.Key)

		if err != nil {
			return err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		p.httpListener, err = tls.Listen("tcp", p.service, tlsConfig)
		if err != nil {
			return err
		}

	} else {

		p.httpListener, err = net.Listen("tcp", p.service)
		if err != nil {
			return err
		}
	}

	go p.httpServer.Serve(p.httpListener)

	return nil
}
