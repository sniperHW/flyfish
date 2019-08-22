package flyfish

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/conf"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	server listener
	stoped int32
)

type Dispatcher interface {
	Dispatch(kendynet.StreamSession, *codec.Message)
	OnClose(kendynet.StreamSession, string)
	OnNewClient(kendynet.StreamSession)
}

type listener interface {
	Close()
	Start() error
}

type tcpListener struct {
	l *tcp.Listener
}

func newTcpListener(nettype, service string) (*tcpListener, error) {
	var err error
	l := &tcpListener{}
	l.l, err = tcp.New(nettype, service)

	if nil == err {
		return l, nil
	} else {
		return nil, err
	}
}

func (this *tcpListener) Close() {
	this.l.Close()
}

func sendLoginResp(session kendynet.StreamSession, loginResp *protocol.LoginResp) bool {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginResp)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func recvLoginReq(session kendynet.StreamSession) (*protocol.LoginReq, error) {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginReq := &protocol.LoginReq{}
			if err = proto.Unmarshal(buffer[2:w], loginReq); err != nil {
				return loginReq, nil
			} else {
				return nil, err
			}
		}
	}
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func (this *tcpListener) Start() error {
	if nil == this.l {
		return fmt.Errorf("invaild listener")
	}
	return this.l.Serve(func(session kendynet.StreamSession) {
		go func() {

			config := conf.GetConfig()

			loginReq, err := recvLoginReq(session)
			if nil != err {
				session.Close("login failed", 0)
				return
			}

			if !verifyLogin(loginReq) {
				session.Close("login failed", 0)
				return
			}

			loginResp := &protocol.LoginResp{
				Ok:       proto.Bool(true),
				Compress: proto.Bool(config.Compress && loginReq.GetCompress()),
			}

			if !sendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			session.SetRecvTimeout(protocol.PingTime * 2)

			//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
			session.SetReceiver(codec.NewReceiver(config.Compress && loginReq.GetCompress()))
			session.SetEncoder(codec.NewEncoder(config.Compress && loginReq.GetCompress()))

			session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
				onClose(sess, reason)
			})
			onNewClient(session)
			session.Start(func(event *kendynet.Event) {
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					msg := event.Data.(*codec.Message)
					dispatch(session, msg)
				}
			})
		}()
	})
}

func getFileList(dirpath string) ([]string, error) {
	var file_list []string
	dir_err := filepath.Walk(dirpath,
		func(path string, f os.FileInfo, err error) error {
			if f == nil {
				return err
			}
			if !f.IsDir() {
				file_list = append(file_list, path)
				return nil
			}

			return nil
		})
	return file_list, dir_err
}

type ByID []string

func (a ByID) Len() int      { return len(a) }
func (a ByID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByID) Less(i, j int) bool {
	l := a[i]
	r := a[j]
	fieldl := strings.Split(l, "_")
	fieldr := strings.Split(r, "_")
	if len(fieldl) != 2 || len(fieldr) != 2 {
		panic("invaild writeBack file")
	}

	idl, err := strconv.ParseInt(strings.TrimRight(fieldl[1], binlogSuffix), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	idr, err := strconv.ParseInt(strings.TrimRight(fieldr[1], binlogSuffix), 10, 64)
	if nil != err {
		panic("invaild writeBack file")
	}

	return idl < idr
}

func sortFileList(fileList []string) {
	sort.Sort(ByID(fileList))
}

func Start() error {

	var err error
	var f *os.File

	config := conf.GetConfig()

	//创建保留文件
	pid := os.Getpid()

	tmpFileName = fmt.Sprintf("%d_%d.tmp", pid, time.Now().Unix())

	f, err = os.Create(tmpFileName)

	if nil != err {
		return err
	}

	//预留1MB空间
	if _, err := f.Write(make([]byte, 1024*1024)); nil != err {
		return err
	}

	if err := f.Sync(); nil != err {
		return err
	}

	f.Close()

	cmdProcessor = cmdProcessorLocalCache{}
	sqlResponse = sqlResponseLocalCache{}

	Debugln(config.DBConfig.SqlType)

	if config.DBConfig.SqlType == "mysql" {
		BinaryToSqlStr = mysqlBinaryToPgsqlStr
		buildInsertUpdateString = buildInsertUpdateStringMySql
	} else {
		BinaryToSqlStr = pgsqlBinaryToPgsqlStr
		buildInsertUpdateString = buildInsertUpdateStringPgSql
	}

	if !initSql() {
		return fmt.Errorf("initSql failed")
	}

	initProcessUnit()

	if !StartReplayBinlog() {
		return fmt.Errorf("StartReplayBinlog failed")
	}

	server, err = newTcpListener("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))
	if nil != err {
		return err
	}

	go func() {
		err := server.Start()
		if nil != err {
			Errorf("server.Start() error:%s\n", err.Error())
		}
		Infoln("flyfish listener stop")
	}()

	Infoln("flyfish start:", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))

	return nil
}

func StopServer() {
	if atomic.CompareAndSwapInt32(&stoped, 0, 1) {
		Infoln("StopServer")
		server.Close()
	}
}

func isStop() bool {
	return atomic.LoadInt32(&stoped) == 1
}

func waitCondition(fn func() bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			if fn() {
				wg.Done()
				break
			}
		}
	}()
	wg.Wait()
}

func Stop() {

	//第一步关闭监听
	StopServer()

	sessions.Range(func(key, value interface{}) bool {
		value.(kendynet.StreamSession).ShutdownRead()
		return true
	})

	Infoln("ShutdownRead ok", "cmdCount:", cmdCount, "totalSqlCount:", totalSqlCount)

	//等待redis请求和命令执行完成
	waitCondition(func() bool {
		if atomic.LoadInt32(&cmdCount) == 0 {
			return true
		} else {
			return false
		}
	})

	stopSql()

	Infoln("sql stop ok")

	//关闭所有客户连接

	sessions.Range(func(key, value interface{}) bool {
		value.(kendynet.StreamSession).Close("", 1)
		return true
	})

	waitCondition(func() bool {
		if atomic.LoadInt32(&clientCount) == 0 {
			return true
		} else {
			return false
		}
	})

	os.Remove(tmpFileName)

	Infoln("flyfish stop ok")

}
