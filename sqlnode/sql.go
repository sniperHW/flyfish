package sqlnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet"
	"log"
	"sync"
	"sync/atomic"

	protocol "github.com/sniperHW/flyfish/proto"
)

var (
	globalListener *net.Listener
	clientCount    int64
	sessions       sync.Map
)

func Start(cfgFilePath string) bool {
	var err error

	if err = initConfig(cfgFilePath); err != nil {
		log.Fatalf("init config: %s.\n", err)
	}

	initLog()
	getLogger().Infoln("init config.")

	if err = initDB(); err != nil {
		getLogger().Fatalf("init db: %s.", err)
		return false
	}
	getLogger().Infoln("init db.")

	if err = initDBMeta(); err != nil {
		getLogger().Fatalf("init db-meta: %s.", err)
		return false
	}
	getLogger().Infoln("init de-meta.")

	initMessageHandler()
	registerMessageHandlers()
	//initMessageRoutine()
	getLogger().Infoln("init message handler.")

	initCmdProcessor()
	getLogger().Infoln("init processor.")

	if err = startListen(); err != nil {
		getLogger().Fatalf("start listen: %s.", err)
		return false
	}
	getLogger().Infof("start listen on %s:%d.", getConfig().ServiceHost, getConfig().ServicePort)

	return true
}

func Stop() {

}

func registerMessageHandlers() {
	registerMessageHandler(uint16(protocol.CmdType_ReloadTableConf), onReloadTableConf)
	registerMessageHandler(uint16(protocol.CmdType_Get), onGet)
}

func startListen() error {
	var err error

	config := getConfig()
	if globalListener, err = net.NewListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort), verifyLogin); err != nil {
		return err
	}

	go func() {
		err := globalListener.Serve(func(session kendynet.StreamSession, compress bool) {
			go func() {
				session.SetRecvTimeout(protocol.PingTime * 2)
				session.SetSendQueueSize(10000)

				//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
				session.SetReceiver(net.NewReceiver(pb.GetNamespace("request"), compress))
				session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))

				session.SetCloseCallBack(onSessionClosed)
				onNewSession(session)

				if err := session.Start(func(event *kendynet.Event) {
					if event.EventType == kendynet.EventTypeError {
						event.Session.Close(event.Data.(error).Error(), 0)
					} else {
						msg := event.Data.(*net.Message)
						dispatchMessage(session, msg.GetCmd(), msg)
					}
				}); err != nil {
					getLogger().Errorf("session start: %s.", err)
				}
			}()
		})

		if err != nil {
			getLogger().Errorf("serve: %s.", err.Error())
		}

		getLogger().Infoln("listen stop.")
	}()

	return nil
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func onNewSession(session kendynet.StreamSession) {
	getLogger().Infof("client connected: remote-addr(%s).", session.RemoteAddr())

	atomic.AddInt64(&clientCount, 1)
	session.SetUserData(newCliConn(session))
	sessions.Store(session, session)
}

func onSessionClosed(session kendynet.StreamSession, reason string) {
	getLogger().Infof("client disconnected: remote-addr(%s) reason(%s).", session.RemoteAddr(), reason)

	if u := session.GetUserData(); nil != u {
		switch u.(type) {
		case *cliConn:
			u.(*cliConn).clear()
		}
	}
	sessions.Delete(session)
	atomic.AddInt64(&clientCount, -1)
}
