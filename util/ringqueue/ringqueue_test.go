package ringqueue

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingQueue(t *testing.T) {

	queue := New(8)

	assert.Nil(t, queue.Front())

	for i := 0; i < 8; i++ {
		assert.Equal(t, queue.Append(i), true)
	}

	assert.Equal(t, queue.Append(9), false)

	//出列4个元素
	for i := 0; i < 4; i++ {
		assert.NotNil(t, queue.PopFront())
	}

	//再次压入4个元素
	for i := 0; i < 4; i++ {
		assert.Equal(t, queue.Append(i), true)
	}

	//此时队列满，添加不应该成功
	assert.Equal(t, queue.Append(9), false)

	assert.Equal(t, queue.w+1, queue.r)

	//出列8个元素
	for i := 0; i < 8; i++ {
		assert.NotNil(t, queue.PopFront())
	}

	assert.Nil(t, queue.Front())

	assert.Equal(t, queue.w, queue.r)

}
