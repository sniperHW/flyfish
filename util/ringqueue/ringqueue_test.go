package ringqueue

import (
	"testing"
)

func TestRingQueue(t *testing.T) {

	queue := New(8)

	if nil != queue.Front() {
		t.Fatal("nil != queue.Front()")
	}

	for i := 0; i < 8; i++ {
		if !queue.Append(i) {
			t.Fatal("Append failed", i)
		}
	}

	if queue.Append(9) {
		t.Fatal("queue is full,but append ok")
	}

	//出列4个元素
	for i := 0; i < 4; i++ {
		if nil == queue.PopFront() {
			t.Fatal("pop failed")
		}
	}

	//再次压入4个元素
	for i := 0; i < 4; i++ {
		if !queue.Append(i) {
			t.Fatal("Append failed", i)
		}
	}

	//此时队列满，添加不应该成功
	if queue.Append(9) {
		t.Fatal("queue is full,but append ok")
	}

	if queue.w+1 != queue.r {
		t.Fatal("queue.w+1 != queue.r")
	}

	//出列8个元素
	for i := 0; i < 8; i++ {
		if nil == queue.PopFront() {
			t.Fatal("pop failed")
		}
	}

	if nil != queue.Front() {
		t.Fatal("queue should be empty")
	}

	if queue.w != queue.r {
		t.Fatal("queue.w != queue.r")
	}

}
