package bitmap

//go test -covermode=count -v -coverprofile=coverage.out
//go tool cover -html=coverage.out

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitmap(t *testing.T) {
	b := New(129)
	assert.Equal(t, 17, len(b.bits))
	b.Set(129)
	assert.Equal(t, false, b.Test(129))
	b.Set(0, 15)

	assert.Equal(t, true, b.Test(0))

	assert.Equal(t, true, b.Test(15))

	assert.Equal(t, []int{0, 15}, b.GetOpenBits())

	b.Set(1, 9, 27, 32, 69, 130, 150)
	b.Clear(15)

	assert.Equal(t, []int{0, 1, 9, 27, 32, 69}, b.GetOpenBits())

}
