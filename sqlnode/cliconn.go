package sqlnode

import (
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/kendynet"
	"sync"
)

//type replier struct {
//}

type cliConn struct {
	mtx     sync.Mutex
	session kendynet.StreamSession
	//commands map[int64]cmd
}

func newCliConn(session kendynet.StreamSession) *cliConn {
	return &cliConn{
		session: session,
		//commands: make(map[int64]cmd),
	}
}

//func (c *cliConn) addCmd(cmd cmd) {
//	c.mtx.Lock()
//	defer c.mtx.Unlock()
//
//	if _, ok := c.commands[cmd.seqNo()]; ok {
//		panic("repeated seqNo")
//	} else {
//		c.commands[cmd.seqNo()] = cmd
//	}
//}
//
//func (c *cliConn) remCmd(cmd cmd) {
//	c.remCmdBySeqNo(cmd.seqNo())
//}
//
//func (c *cliConn) remCmdBySeqNo(seqNo int64) {
//	c.mtx.Lock()
//	defer c.mtx.Unlock()
//
//	if _, ok := c.commands[seqNo]; ok {
//		delete(c.commands, seqNo)
//	}
//}

func (c *cliConn) clear() {
	//c.commands = make(map[int64]cmd)
}

//func (c *cliConn) isCmdExist(seqNo int64) bool {
//	c.mtx.Lock()
//	defer c.mtx.Unlock()
//	return c.commands[seqNo] != nil
//}

func (c *cliConn) sendMessage(msg *net.Message) error {
	return c.session.Send(msg)
}
