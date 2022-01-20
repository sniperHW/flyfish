package flypd

import (
	"encoding/json"
	"fmt"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"net/http"
	"strings"
	"time"
)

type httpReplyer struct {
	w http.ResponseWriter
}

func (h *httpReplyer) reply(resp *snet.Message) {
	if jsonByte, err := json.Marshal(resp.Msg); nil == err {
		h.w.Write(jsonByte)
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
	if !p.isLeader() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("pd is not leader"))
		return
	}

	if tmp := strings.Split(r.URL.Path, "/"); len(tmp) == 2 {
		if tmp[1] == "QueryPdLeader" {
			if jsonByte, err := json.Marshal(&sproto.QueryPdLeaderResp{
				Yes: p.isLeader(),
			}); nil == err {
				w.Write(jsonByte)
			}
		} else {
			req, err := p.fetchReq(tmp[1], r)
			if nil == err {
				p.onMsg(&httpReplyer{w: w}, req)
			} else {
				GetSugar().Errorf("ServeHTTP error:%v", err)
			}
		}
	}
}

func (p *pd) startHttpService() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", p.service)
	if err != nil {
		return err
	}

	p.httpListener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	p.httpServer = &http.Server{
		Addr:           p.service,
		Handler:        p,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go p.httpServer.Serve(p.httpListener)

	return nil
}
