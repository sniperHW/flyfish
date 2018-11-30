package tcp

import (
    "net"
    "sync/atomic"
    "github.com/sniperHW/kendynet"
    "github.com/sniperHW/kendynet/socket/stream_socket"
)

type Listener struct{
    listener    *net.TCPListener
    started      int32
}

func NewListener(nettype,service string) (*Listener,error) {
    tcpAddr,err := net.ResolveTCPAddr(nettype, service)
    if err != nil{
        return nil,err
    }
    listener, err := net.ListenTCP(nettype, tcpAddr)
    if err != nil{
        kendynet.Errorf("ListenTCP service:%s error:%s\n",service,err.Error())
        return nil,err
    }
    return &Listener{listener:listener},nil
}

func (this *Listener) Close() {
    if nil != this.listener {
        this.listener.Close()
    }
}


func (this *Listener) Start(onNewClient func(kendynet.StreamSession)) error {

    if nil == onNewClient {
        return kendynet.ErrInvaildNewClientCB
    }

    if !atomic.CompareAndSwapInt32(&this.started,0,1) {
        return kendynet.ErrServerStarted
    }

    for {
        conn, err := this.listener.Accept()
        if err != nil {
            this.listener.Close()
            return err
        }
        onNewClient(stream_socket.NewStreamSocket(conn))
    }
}