// +build darwin netbsd freebsd openbsd dragonfly

package aiogo

import (
	"sync"
	"sync/atomic"
	"syscall"
)

type kqueue struct {
	fd        int
	fd2Conn   fd2Conn
	ver       int64
	closeOnce sync.Once
}

func openPoller() (*kqueue, error) {
	kfd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	poller := new(kqueue)
	poller.fd = kfd
	poller.fd2Conn = fd2Conn(make([]sync.Map, hashSize))

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

func (p *kqueue) trigger() error {
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{{
		Ident:  0,
		Filter: syscall.EVFILT_USER,
		Fflags: syscall.NOTE_TRIGGER,
	}}, nil, nil)
	return err
}

func (p *kqueue) enableWrite(c *Conn) bool {
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{{Ident: uint64(c.fd), Flags: syscall.EV_ENABLE, Filter: syscall.EVFILT_WRITE}}, nil, nil)
	return nil == err
}

func (p *kqueue) disableWrite(c *Conn) bool {
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{{Ident: uint64(c.fd), Flags: syscall.EV_DISABLE, Filter: syscall.EVFILT_WRITE}}, nil, nil)
	return nil == err
}

func (p *kqueue) watch(conn *Conn) bool {

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

func (p *kqueue) unwatch(conn *Conn) bool {

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

func (p *kqueue) wait(stoped *int32) {

	defer syscall.Close(p.fd)

	eventlist := make([]syscall.Kevent_t, 64)

	for atomic.LoadInt32(stoped) == 0 {

		n, err0 := syscall.Kevent(p.fd, nil, eventlist, nil)

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

		if n > 0 {
			for i := 0; i < n; i++ {
				e := &eventlist[i]
				fd := int(e.Ident)
				if fd != 0 {
					if conn, ok := p.fd2Conn.get(fd); ok && conn.pollerVersion != pollerVersion {
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

						op := gOpPool.get()
						op.tt = pollEvent
						op.c = conn
						op.ud = event

						if nil != conn.worker.push(op) {
							return
						}
					}
				}
			}

			if n == len(eventlist) {
				eventlist = make([]syscall.Kevent_t, n<<1)
			}
		}
	}
}
