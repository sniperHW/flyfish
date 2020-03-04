package fixedarray

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFixedArray(t *testing.T) {
	pool := NewPool(100)
	array := pool.Get()

	assert.Equal(t, array.Len(), 0)
	assert.Equal(t, array.Cap(), 100)
	assert.Equal(t, array.Empty(), true)

	if array.Len() != 0 || array.Cap() != 100 {
		t.Fatal("array.Len() != 0 || array.Cap() != 100")
	}

	for i := 0; i < 100; i++ {
		array.Append(i + 1)
	}

	assert.Equal(t, array.Append(100), false)

	assert.Equal(t, array.Len(), 100)
	assert.Equal(t, array.Cap(), 100)
	assert.Equal(t, array.Full(), true)

	array.ForEach(func(v interface{}) {

	})

	array.Reset()
	assert.Equal(t, array.Len(), 0)

	pool.Put(array)

}
