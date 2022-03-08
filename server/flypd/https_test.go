package flypd

//go test -run=.

import (
	"crypto/tls"
	//"crypto/x509"
	"fmt"
	"io/ioutil"
	//"net"
	"net/http"
	"testing"
	"time"
)

type handler struct {
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
}

//openssl genrsa -out server.key 2048
//openssl req -new -x509 -key server.key -out server.crt -days 365

func client(t *testing.T) {
	tr := &http.Transport{
		//把从服务器传过来的非叶子证书，添加到中间证书的池中，使用设置的根证书和中间证书对叶子证书进行验证。
		//TLSClientConfig: &tls.Config{RootCAs: pool},
		//InsecureSkipVerify用来控制客户端是否证书和服务器主机名。如果设置为true,
		//则不会校验证书以及证书中的主机名和服务器主机名是否一致。
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get("https://127.0.0.1:8100")
	if err != nil {
		fmt.Println("Get error:", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))

}

func TestHttps(t *testing.T) {
	cert, _ := tls.LoadX509KeyPair("test/server.crt", "test/server.key")

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	httpListener, err := tls.Listen("tcp", ":8100", tlsConfig)
	if err != nil {
		panic(err)
	}

	httpServer := &http.Server{
		Handler:        handler{},
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go httpServer.Serve(httpListener)

	client(t)

}
