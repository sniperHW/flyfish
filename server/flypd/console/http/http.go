package http

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

type Client struct {
	service string
	c       *http.Client
}

func NewClient(service string) *Client {
	return &Client{
		service: service,
		c:       &http.Client{Timeout: time.Second * 15},
	}
}

func NewTlsClient(service string) *Client {
	return &Client{
		service: service,
		c: &http.Client{
			Timeout: time.Second * 15,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}},
	}
}

func (c Client) Call(req proto.Message, resp proto.Message) (proto.Message, error) {
	cmd := strings.Split(reflect.TypeOf(req).String(), ".")[1]
	j, err := proto.Marshal(req)
	if nil != err {
		return nil, err
	}
	body := bytes.NewBufferString(string(j))
	httpreq, err := http.NewRequest("Post", fmt.Sprintf("http://%s/%s", c.service, cmd), body)
	if nil != c.c.Transport {
		httpreq.Header.Add("token", "feiyu_tech_2021")
	}

	httpresp, err := c.c.Do(httpreq)
	if err != nil {
		return nil, err
	}

	defer httpresp.Body.Close()
	data, err := ioutil.ReadAll(httpresp.Body)

	if nil != err {
		return nil, err
	}

	if httpresp.StatusCode != 200 {
		return nil, fmt.Errorf(httpresp.Status)
	}

	err = proto.Unmarshal(data, resp)
	return resp, err
}
