package list

type Element struct {
	Value interface{}
	pprev *Element
	nnext *Element
	l     *List
}

func (e *Element) Next() *Element {
	if nil == e.l {
		return nil
	}

	if e.nnext == &e.l.head {
		return nil
	}

	return e.nnext
}

func (e *Element) Prev() *Element {
	if nil == e.l {
		return nil
	}

	if e.pprev == &e.l.head {
		return nil
	}

	return e.pprev
}

func (e *Element) List() *List {
	return e.l
}

type List struct {
	head Element
	size int
}

func New() *List {
	l := &List{}
	l.head.pprev = &l.head
	l.head.nnext = &l.head
	return l
}

func (l *List) Len() int {
	return l.size
}

func (l *List) Remove(e *Element) {
	if l != e.l {
		panic("l != e.l")
	}
	pprev := e.pprev
	nnext := e.nnext
	pprev.nnext = nnext
	nnext.pprev = pprev
	e.pprev = nil
	e.nnext = nil
	e.l = nil
	l.size--
}

func (l *List) PushBack(e *Element) {
	if nil != e.l {
		panic("nil != e.l")
	}

	last := l.head.pprev
	last.nnext = e
	e.pprev = last
	e.nnext = &l.head
	l.head.pprev = e
	e.l = l
	l.size++
}

func (l *List) Back() *Element {
	last := l.head.pprev
	if last == &l.head {
		return nil
	} else {
		return last
	}
}

func (l *List) PushFront(e *Element) {
	if nil != e.l {
		panic("nil != e.l")
	}

	first := l.head.nnext
	first.pprev = e
	e.nnext = first
	e.pprev = &l.head
	l.head.nnext = e
	e.l = l
	l.size++
}

func (l *List) Front() *Element {
	first := l.head.nnext
	if first == &l.head {
		return nil
	} else {
		return first
	}
}
