package list

//go test -covermode=count -v -coverprofile=coverage.out
//go tool cover -html=coverage.out

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type E struct {
	le    Element
	value int
}

func TestList(t *testing.T) {
	l := New()
	assert.Equal(t, l.Len(), 0)
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())

	e1 := &E{
		value: 1,
	}
	e1.le.Value = e1

	l.PushBack(&e1.le)
	assert.Equal(t, l.Len(), 1)
	assert.Equal(t, l.Front(), &e1.le)
	assert.Equal(t, l.Back(), &e1.le)

	assert.Nil(t, e1.le.Next())
	assert.Nil(t, e1.le.Prev())

	l.Remove(&e1.le)

	assert.Equal(t, l.Len(), 0)
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())

	assert.Nil(t, e1.le.Next())
	assert.Nil(t, e1.le.Prev())

	e := e1.le.Value.(*E)

	assert.Equal(t, e, e1)

	{

		e1 := &E{
			value: 1,
		}
		e1.le.Value = e1

		e2 := &E{
			value: 2,
		}
		e2.le.Value = e2

		e3 := &E{
			value: 3,
		}
		e3.le.Value = e3

		l.PushBack(&e1.le)
		l.PushBack(&e2.le)
		l.PushBack(&e3.le)

		assert.Equal(t, l.Len(), 3)

		assert.Equal(t, l.Front(), &e1.le)
		assert.Equal(t, l.Back(), &e3.le)

		assert.Equal(t, e1.le.Next(), &e2.le)
		assert.Nil(t, e1.le.Prev())

		assert.Equal(t, e2.le.Next(), &e3.le)
		assert.Equal(t, e2.le.Prev(), &e1.le)

		assert.Equal(t, e3.le.Prev(), &e2.le)
		assert.Nil(t, e3.le.Next())

		l.Remove(&e2.le)

		assert.Equal(t, l.Len(), 2)
		assert.Equal(t, l.Front(), &e1.le)
		assert.Equal(t, l.Back(), &e3.le)

		assert.Equal(t, e1.le.Next(), &e3.le)
		assert.Nil(t, e1.le.Prev())

		assert.Equal(t, e3.le.Prev(), &e1.le)
		assert.Nil(t, e3.le.Next())

		l.Remove(&e1.le)

		assert.Equal(t, l.Len(), 1)
		assert.Equal(t, l.Front(), &e3.le)
		assert.Equal(t, l.Back(), &e3.le)

		l.Remove(&e3.le)
		assert.Equal(t, l.Len(), 0)
		assert.Nil(t, l.Front())
		assert.Nil(t, l.Back())

		assert.Nil(t, e1.le.Next())
		assert.Nil(t, e1.le.Prev())

		assert.Nil(t, e2.le.Next())
		assert.Nil(t, e2.le.Prev())

		assert.Nil(t, e3.le.Next())
		assert.Nil(t, e3.le.Prev())

	}

	{

		e1 := &E{
			value: 1,
		}
		e1.le.Value = e1

		e2 := &E{
			value: 2,
		}
		e2.le.Value = e2

		e3 := &E{
			value: 3,
		}
		e3.le.Value = e3

		l.PushFront(&e3.le)
		l.PushFront(&e2.le)
		l.PushFront(&e1.le)

		assert.Equal(t, l.Len(), 3)

		assert.Equal(t, l.Front(), &e1.le)
		assert.Equal(t, l.Back(), &e3.le)

		assert.Equal(t, e1.le.Next(), &e2.le)
		assert.Nil(t, e1.le.Prev())

		assert.Equal(t, e2.le.Next(), &e3.le)
		assert.Equal(t, e2.le.Prev(), &e1.le)

		assert.Equal(t, e3.le.Prev(), &e2.le)
		assert.Nil(t, e3.le.Next())

		l.Remove(&e2.le)

		assert.Equal(t, l.Len(), 2)
		assert.Equal(t, l.Front(), &e1.le)
		assert.Equal(t, l.Back(), &e3.le)

		assert.Equal(t, e1.le.Next(), &e3.le)
		assert.Nil(t, e1.le.Prev())

		assert.Equal(t, e3.le.Prev(), &e1.le)
		assert.Nil(t, e3.le.Next())

		l.Remove(&e1.le)

		assert.Equal(t, l.Len(), 1)
		assert.Equal(t, l.Front(), &e3.le)
		assert.Equal(t, l.Back(), &e3.le)

		l.Remove(&e3.le)
		assert.Equal(t, l.Len(), 0)
		assert.Nil(t, l.Front())
		assert.Nil(t, l.Back())

		assert.Nil(t, e1.le.Next())
		assert.Nil(t, e1.le.Prev())

		assert.Nil(t, e2.le.Next())
		assert.Nil(t, e2.le.Prev())

		assert.Nil(t, e3.le.Next())
		assert.Nil(t, e3.le.Prev())

	}

}
