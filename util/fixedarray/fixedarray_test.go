package fixedarray

import (
	"fmt"
	"testing"
)

func TestFixedArray(t *testing.T) {
	pool := NewPool(100)
	array := pool.Get()

	if array.Len() != 0 || array.Cap() != 100 {
		t.Fatal("array.Len() != 0 || array.Cap() != 100")
	}

	for i := 0; i < 101; i++ {
		array.Append(i + 1)
	}

	if array.Len() != 100 || array.Cap() != 100 {
		t.Fatal("array.Len() != 100 || array.Cap() != 100")
	}

	array.ForEach(func(v interface{}) {
		fmt.Println(v.(int))
	})

}
