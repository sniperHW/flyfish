// +build linux

package aiogo

import (
	"sync"
	"sync/atomic"
	"syscall"
)

type epoll struct {
	fd        int
	wfd       int // wake fd
	fd2Conn   fd2Conn
	ver       int64
	closeOnce sync.Once
}

func openPoller() (*epoll, error) {
	epollFD, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	poller := new(epoll)
	poller.fd = epollFD
	poller.fd2Conn = fd2Conn(make([]sync.Map, hashSize))

	r0, _, e0 := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if e0 != 0 {
		syscall.Close(epollFD)
		return nil, e0
	}

	if err := syscall.SetNonblock(int(r0), true); err != nil {
		syscall.Close(int(r0))
		syscall.Close(epollFD)
		return nil, e0
	}

	if err := syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, int(r0),
		&syscall.EpollEvent{Fd: int32(r0),
			Events: syscall.EPOLLIN,
		},
	); err != nil {
		syscall.Close(epollFD)
		syscall.Close(int(r0))
		return nil, err
	}

	poller.wfd = int(r0)

	return poller, nil
}

func (p *epoll) trigger() error {
	_, err := syscall.Write(p.wfd, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

const EPOLLET uint32 = 0x80000000

func (p *epoll) enableWrite(c *Conn) bool {
	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_MOD, c.fd, &syscall.EpollEvent{Fd: int32(c.fd), Events: syscall.EPOLLRDHUP | syscall.EPOLLIN | syscall.EPOLLOUT | EPOLLET})
	return nil == err
}

func (p *epoll) disableWrite(c *Conn) bool {
	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_MOD, c.fd, &syscall.EpollEvent{Fd: int32(c.fd), Events: syscall.EPOLLRDHUP | syscall.EPOLLIN | EPOLLET})
	return nil == err
}

func (p *epoll) watch(conn *Conn) bool {

	if _, ok := p.fd2Conn.get(conn.fd); ok {
		return false
	}

	var pollerVersion int32

	for {
		ver := atomic.LoadInt64(&p.ver)
		addVer := int32(ver>>32) + 1
		pollerVersion = int32(ver & 0x00000000FFFFFFFF)
		nextVer := int64(addVer<<32) | int64(pollerVersion)
		if atomic.CompareAndSwapInt64(&p.ver, ver, nextVer) {
			break
		}
	}

	conn.pollerVersion = pollerVersion
	p.fd2Conn.add(conn)

	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, int(conn.fd), &syscall.EpollEvent{Fd: int32(conn.fd), Events: syscall.EPOLLRDHUP | syscall.EPOLLIN | /*syscall.EPOLLOUT |*/ EPOLLET})
	if nil != err {
		p.fd2Conn.remove(conn)
		return false
	} else {
		return true
	}

}

func (p *epoll) unwatch(conn *Conn) bool {

	if _, ok := p.fd2Conn.get(conn.fd); !ok {
		return false
	}

	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_DEL, conn.fd, nil)
	if nil == err {
		p.fd2Conn.remove(conn)
		return true
	} else {
		return false
	}
}

const (
	readEvents  = int(syscall.EPOLLIN)
	writeEvents = int(syscall.EPOLLOUT)
	errorEvents = int(syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP)
)

func (p *epoll) wait(stoped *int32) {

	defer func() {
		syscall.Close(p.fd)
		syscall.Close(p.wfd)
	}()

	eventlist := make([]syscall.EpollEvent, 64)

	for atomic.LoadInt32(stoped) == 0 {
		n, err0 := syscall.EpollWait(p.fd, eventlist, -1)

		if err0 == syscall.EINTR {
			continue
		}

		if err0 != nil && err0 != syscall.EINTR {
			panic(err0)
			return
		}

		var pollerVersion int32

		for {
			ver := atomic.LoadInt64(&p.ver)
			addVer := int32(ver >> 32)
			pollerVersion = int32(ver&0x00000000FFFFFFFF) + 1
			nextVer := int64(addVer<<32) | int64(pollerVersion)
			if atomic.CompareAndSwapInt64(&p.ver, ver, nextVer) {
				break
			}
		}

		for i := 0; i < n; i++ {

			e := &eventlist[i]

			fd := int(e.Fd)

			if fd != p.wfd {

				if conn, ok := p.fd2Conn.get(fd); ok && conn.pollerVersion != pollerVersion {

					event := int(0)

					if e.Events&uint32(errorEvents) != 0 {
						event |= EV_ERROR
					}

					if e.Events&uint32(readEvents) != 0 {
						event |= EV_READ
					}

					if e.Events&uint32(writeEvents) != 0 {
						event |= EV_WRITE
					}

					op := gOpPool.get()
					op.tt = pollEvent
					op.c = conn
					op.ud = event

					if nil != conn.worker.push(op) {
						return
					}
				}

			} else {
				buff := make([]byte, 8)
				for {
					if _, err := syscall.Read(p.wfd, buff); err == syscall.EAGAIN {
						break
					}
				}
			}
		}

		if n == len(eventlist) {
			eventlist = make([]syscall.EpollEvent, n<<1)
		}

	}
}
