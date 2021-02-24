package goaio

import (
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

const hashMask int = 8
const hashSize int = 1 << hashMask

type fd2Conn []sync.Map

func (self *fd2Conn) add(conn *AIOConn) {
	(*self)[conn.fd>>hashMask].Store(conn.fd, reflect.ValueOf(conn).Pointer())
}

func (self *fd2Conn) get(fd int) (*AIOConn, bool) {
	v, ok := (*self)[fd>>hashMask].Load(fd)
	if ok {
		return (*AIOConn)(unsafe.Pointer(v.(uintptr))), true
	} else {
		return nil, false
	}
}

func (self *fd2Conn) remove(conn *AIOConn) {
	(*self)[conn.fd>>hashMask].Delete(conn.fd)
}

type poller_base struct {
	fd      int
	fd2Conn fd2Conn
	ver     int64
	die     chan struct{}
}

func (this *poller_base) updatePollerVersionOnWatch() int32 {
	var pollerVersion int32

	for {
		ver := atomic.LoadInt64(&this.ver)
		addVer := int32(ver>>32) + 1
		pollerVersion = int32(ver & 0x00000000FFFFFFFF)
		nextVer := int64(addVer<<32) | int64(pollerVersion)
		if atomic.CompareAndSwapInt64(&this.ver, ver, nextVer) {
			break
		}
	}
	return pollerVersion
}

func (this *poller_base) updatePollerVersionOnWait() int32 {
	var pollerVersion int32

	for {
		ver := atomic.LoadInt64(&this.ver)
		addVer := int32(ver >> 32)
		pollerVersion = int32(ver&0x00000000FFFFFFFF) + 1
		nextVer := int64(addVer<<32) | int64(pollerVersion)
		if atomic.CompareAndSwapInt64(&this.ver, ver, nextVer) {
			break
		}
	}

	return pollerVersion
}

type pollerI interface {
	close()
	trigger() error
	watch(*AIOConn) bool
	unwatch(*AIOConn) bool
	wait(*int32)
	enableWrite(*AIOConn) bool
	disableWrite(*AIOConn) bool
}
