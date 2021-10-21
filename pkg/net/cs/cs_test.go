package cs

//go test -tags=bio -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
import (
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"testing"
	"time"
)

func TestEcho(t *testing.T) {
	l, _ := NewListener("tcp", "127.0.0.1:8010", 65535, func(_ *flyproto.LoginReq) bool {
		return true
	})

	l.Serve(func(s *flynet.Socket) {
		fmt.Println("on newclient")

		s.SetInBoundProcessor(NewReqInboundProcessor())
		s.SetEncoder(&RespEncoder{})

		s.SetCloseCallBack(func(session *flynet.Socket, reason error) {
			fmt.Println(reason)
		})

		s.BeginRecv(func(s *flynet.Socket, v interface{}) {
			msg := v.(*ReqMessage)
			cmd := msg.Cmd
			fmt.Println("recv msg", cmd, flyproto.CmdType_Ping)
			switch cmd {
			case flyproto.CmdType_Ping:
				err := s.Send(&RespMessage{
					Err: errcode.New(errcode.Errcode_retry, "retry"),
					Data: &flyproto.PingResp{
						Timestamp: time.Now().UnixNano(),
					},
				})
				if nil != err {
					fmt.Println(err)
				}
			default:
			}
		})
	})

	c := NewConnector("tcp", "127.0.0.1:8010", 65535)
	cc, err := c.Dial(time.Second)
	if nil != err {
		return
	} else {

		cc.SetInBoundProcessor(NewRespInboundProcessor())
		cc.SetEncoder(&ReqEncoder{})

		req := &ReqMessage{
			Data: &flyproto.PingReq{
				Timestamp: time.Now().UnixNano(),
			},
		}

		ok := make(chan struct{})

		cc.SetCloseCallBack(func(session *flynet.Socket, reason error) {
			fmt.Println("close", reason)
			close(ok)
		})

		if err := cc.Send(req); nil != err {
			fmt.Println(err)
			return
		}

		cc.Send(&ReqMessage{
			Data: &flyproto.PingReq{
				Timestamp: time.Now().UnixNano(),
			},
		})

		count := 0

		fmt.Println("send")

		cc.BeginRecv(func(s *flynet.Socket, v interface{}) {
			fmt.Println("recv resp")
			msg := v.(*RespMessage)
			cmd := msg.Cmd
			fmt.Println(msg.Err)
			if cmd == flyproto.CmdType_Ping {
				count++
				if count > 0 {
					s.Close(nil, 0)
					return
				}
				cc.Send(req)
			} else {
				panic("error")
			}
		})

		<-ok
	}
}
