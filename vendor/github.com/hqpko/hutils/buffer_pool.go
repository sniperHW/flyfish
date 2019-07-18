package hutils

import (
	"sync"

	"github.com/hqpko/hbuffer"
)

type BufferPool struct {
	pool *sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{pool: &sync.Pool{New: func() interface{} {
		return hbuffer.NewBuffer()
	}}}
}

func (b *BufferPool) Get() *hbuffer.Buffer {
	return b.pool.Get().(*hbuffer.Buffer)
}

func (b *BufferPool) Put(buf *hbuffer.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	b.pool.Put(buf)
}
