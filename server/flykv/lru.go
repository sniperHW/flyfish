package flykv

type lruElement struct {
	pprev    *lruElement
	nnext    *lruElement
	keyvalue *kv
}

type lruList struct {
	head lruElement
	tail lruElement
}

func (this *lruList) init() {
	this.head.nnext = &this.tail
	this.tail.pprev = &this.head
}

/*
 * lru每个kv被访问后重新插入列表头部，尾部表示最久未被访问的kv，可以从cache中kick
 */
func (this *lruList) update(e *lruElement) {
	if e.nnext != nil || e.pprev != nil {
		//先移除
		e.pprev.nnext = e.nnext
		e.nnext.pprev = e.pprev
		e.nnext = nil
		e.pprev = nil
	}

	//插入头部
	e.nnext = this.head.nnext
	e.nnext.pprev = e
	e.pprev = &this.head
	this.head.nnext = e

}

func (this *lruList) remove(e *lruElement) {
	if e.nnext != nil && e.pprev != nil {
		e.pprev.nnext = e.nnext
		e.nnext.pprev = e.pprev
		e.nnext = nil
		e.pprev = nil
	}
}
