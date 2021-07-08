// +build darwin netbsd freebsd openbsd dragonfly

package goaio

import (
	"fmt"
	"syscall"
)

type kqueue struct {
	poller_base
}

func openPoller() (*kqueue, error) {
	kfd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	poller := new(kqueue)
	poller.fd = kfd

	_, err = syscall.Kevent(poller.fd, []syscall.Kevent_t{{
		Ident:  0,
		Filter: syscall.EVFILT_USER,
		Flags:  syscall.EV_ADD | syscall.EV_CLEAR,
	}}, nil, nil)

	if err != nil {
		syscall.Close(kfd)
		return nil, err
	}

	return poller, nil
}

func (p *kqueue) close() {
	p.trigger()
}

func (p *kqueue) trigger() error {
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{{
		Ident:  0,
		Filter: syscall.EVFILT_USER,
		Fflags: syscall.NOTE_TRIGGER,
	}}, nil, nil)
	return err
}

func (p *kqueue) _watch(conn *AIOConn) bool {

	if _, ok := p.fd2Conn.get(conn.fd); ok {
		return false
	}

	p.fd2Conn.add(conn)

	events := []syscall.Kevent_t{
		syscall.Kevent_t{Ident: uint64(conn.fd), Flags: syscall.EV_ADD | syscall.EV_CLEAR, Filter: syscall.EVFILT_READ},
		syscall.Kevent_t{Ident: uint64(conn.fd), Flags: syscall.EV_ADD | syscall.EV_CLEAR /*| syscall.EV_DISABLE*/, Filter: syscall.EVFILT_WRITE},
	}

	if _, err := syscall.Kevent(p.fd, events, nil, nil); err != nil {
		p.fd2Conn.remove(conn)
		return false
	} else {
		return true
	}
}

func (p *kqueue) watch(conn *AIOConn) <-chan bool {
	ch := watch(&p.poller_base, conn)
	p.trigger()
	return ch
}

func (p *kqueue) unwatch(conn *AIOConn) bool {

	if _, ok := p.fd2Conn.get(conn.fd); !ok {
		return false
	}

	events := []syscall.Kevent_t{
		syscall.Kevent_t{Ident: uint64(conn.fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_READ},
		syscall.Kevent_t{Ident: uint64(conn.fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_WRITE},
	}

	if _, err := syscall.Kevent(p.fd, events, nil, nil); err != nil {
		return false
	} else {
		p.fd2Conn.remove(conn)
		return true
	}
}

func (p *kqueue) wait(die <-chan struct{}) {

	defer func() {
		syscall.Close(p.fd)
	}()

	eventlist := make([]syscall.Kevent_t, 64)

	for {
		select {
		case <-die:
			return
		default:

			doWatch(&p.poller_base, p._watch)

			n, err0 := syscall.Kevent(p.fd, nil, eventlist, nil)

			if err0 == syscall.EINTR {
				continue
			}

			if err0 != nil && err0 != syscall.EINTR {
				panic(fmt.Errorf("syscall.Kevent error:%v", err0))
				return
			}

			if n > 0 {
				for i := 0; i < n; i++ {
					e := &eventlist[i]
					fd := int(e.Ident)
					if fd != 0 {
						if conn, ok := p.fd2Conn.get(fd); ok {
							event := int(0)
							if (e.Flags&syscall.EV_EOF != 0) || (e.Flags&syscall.EV_ERROR != 0) {
								event |= EV_ERROR
							}

							if e.Filter == syscall.EVFILT_READ {
								event |= EV_READ
							}

							if e.Filter == syscall.EVFILT_WRITE {
								event |= EV_WRITE
							}

							conn.onActive(event)
						}
					}
				}

				if n == len(eventlist) {
					eventlist = make([]syscall.Kevent_t, n<<1)
				}
			}
		}
	}
}

func rawRead(fd int, p []byte) (n int, err error) {
	return syscall.Read(fd, p)
}

func rawWrite(fd int, p []byte) (n int, err error) {
	return syscall.Write(fd, p)
}
