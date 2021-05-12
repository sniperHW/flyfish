# goaio

golang proactor模型网络库

使用epoll/kqueue实现

goaio使用少量goroutine实现网络服务，大大减少了goroutine的数量。

对于大多数长连接服务，同时可接收数据的连接占比非常小，对于每连接一个goroutine的模型，因为read调用是阻塞的，即使连接上没有数据可读，也必须分配buff，阻塞在read上等待数据的到来。

对于这种情况，goaio可以使用sharebuff模式，在recv请求中不提供buff，当连接可读时才从buffpool获取buff用以接收，buff处理完之后返回buffpool以供后续使用。通过sharebuff模式可以大大减少大量长连接占用的内存数量。

### API

```go
func NewAIOService(worker int) *AIOService
```

创建一个AIO服务，服务会创建worker数量goroutine用以处理io操作。

```go
type AIOConnOption struct {
	SendqueSize int
	RecvqueSize int
	ShareBuff   ShareBuffer
	UserData    interface{}
}

func (this *AIOService) Bind(conn net.Conn, option AIOConnOption) (*AIOConn, error)
```

将`net.Conn`对象绑定到`AIOService`,如果提供了`ShareBuff`则当连接可读，而`recv`请求又没有提供`buff`时将通过`ShareBuff`获取`buff`以接收网络数据。

```go
func (this *AIOService) GetCompleteStatus() (AIOResult, error)
```

获取操作结果。需要注意的是，如果使用多个`goroutine`调用`AIOService.GetCompleteStatus`，对于一个连接，如果同时投递了多个io操作请求，则操作结果可能会以任意顺序被多个`goroutine`获取到。如果操作结果的顺序是重要的，要么避免同时投递多个io请求，要么只使用一个`goroutine`获取操作结果。

```go
func (this *AIOConn) Recv(context interface{}, buff []byte) error
```

投递读请求。如果没有提供`buff`,且没有提供`ShareBuff`，当连接可读时将使用`make`创建一个`buff`来接收数据。数据的接收使用readv接口，因此，会一次性将多个buff提供给readv。

```go
func (this *AIOConn) Send(context interface{}, buff []byte) error
```

投递写请求。数据的接收使用writev接口，因此，会一次性将多个buff提供给writev。只有当操作提供的所有buff都发送完毕，整个操作才算完成。

对于设置了发送超时的情况，返回的结果包含超时错误以及成功发送的字节数，使用者可以根据情况重发剩余部分。

### 关于GatherIO

goaio默认是singleIO模式，即Send和Recv只能传进一个buff。每次io只能处理一个buff。

除此之外还提供了GatherIO模式，只要使用-tags=gatherIO编译即可。在此模式下Send和Recv可以接受多个buff。使用writev/readv处理io请求。但此模式Send/Recv会有一个额外的`[][]byte{}`创建消耗，所以只有在确实需要这个功能时才开启。



### 示例程序

```go
package main

import (
	"fmt"
	"github.com/sniperHW/goaio"
	"net"
	"time"
)

func main() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:8110")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			res, err := goaio.GetCompleteStatus()
			if nil != err {
				return
			} else if nil != res.Err {
				fmt.Println("go error", res.Err)
				res.Conn.Close(res.Err)
			} else if res.Context.(rune) == 'r' {
				res.Conn.Send('w', res.Buff[:res.Bytestransfer])
			} else {
				res.Conn.Recv('r', res.Buff[:cap(res.Buff)])
			}
		}
	}()

	fmt.Println("server start at localhost:8110")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("new client")

		c, _ := goaio.Bind(conn, goaio.AIOConnOption{})

		c.SetRecvTimeout(time.Second * 5)

		c.Recv('r', make([]byte, 1024*4))

	}

}
```

