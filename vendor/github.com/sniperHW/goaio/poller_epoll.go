// +build linux

package goaio

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	readEvents         = int(syscall.EPOLLIN)
	writeEvents        = int(syscall.EPOLLOUT)
	errorEvents        = int(syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP)
	EPOLLET     uint32 = 0x80000000
)

type epoll struct {
	poller_base
	wfd int // wake fd
}

func openPoller() (*epoll, error) {
	epollFD, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	poller := new(epoll)
	poller.fd = epollFD

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

func (p *epoll) close() {
	p.trigger()
}

func (p *epoll) trigger() error {
	_, err := syscall.Write(p.wfd, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

func (p *epoll) _watch(conn *AIOConn) bool {

	if _, ok := p.fd2Conn.get(conn.fd); ok {
		return false
	}

	p.fd2Conn.add(conn)

	err := syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, int(conn.fd), &syscall.EpollEvent{Fd: int32(conn.fd), Events: syscall.EPOLLRDHUP | syscall.EPOLLIN | syscall.EPOLLOUT | EPOLLET})
	if nil != err {
		p.fd2Conn.remove(conn)
		return false
	} else {
		return true
	}
}

func (p *epoll) watch(conn *AIOConn) <-chan bool {
	ch := watch(&p.poller_base, conn)
	p.trigger()
	return ch
}

func (p *epoll) unwatch(conn *AIOConn) bool {

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

func (p *epoll) wait(die <-chan struct{}) {

	defer func() {
		syscall.Close(p.fd)
		syscall.Close(p.wfd)
	}()

	eventlist := make([]syscall.EpollEvent, 64)

	for {
		select {
		case <-die:
			return
		default:

			doWatch(&p.poller_base, p._watch)

			n, err0 := syscall.EpollWait(p.fd, eventlist, -1)

			if err0 == syscall.EINTR {
				continue
			}

			if err0 != nil && err0 != syscall.EINTR {
				panic(fmt.Errorf("syscall.EpollWait error:%v", err0))
				return
			}

			for i := 0; i < n; i++ {

				e := &eventlist[i]

				fd := int(e.Fd)

				if fd != p.wfd {

					if conn, ok := p.fd2Conn.get(fd); ok {

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

						conn.onActive(event)
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
}

// raw read for nonblocking op to avert context switch
func rawRead(fd int, p []byte) (n int, err error) {
	r0, _, e1 := syscall.RawSyscall(syscall.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}

// raw write for nonblocking op to avert context switch
func rawWrite(fd int, p []byte) (n int, err error) {
	r0, _, e1 := syscall.RawSyscall(syscall.SYS_WRITE, uintptr(fd), uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)))
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}
