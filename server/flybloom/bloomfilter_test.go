package flybloom

import (
	"fmt"
	"github.com/sniperHW/flyfish/pkg/compress"
	"github.com/sniperHW/flyfish/server/flybloom/bloomfilter"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"unsafe"
)

const (
	maxElements = 100000000
	probCollide = 0.001
)

func test1(t *testing.T) {

	bf, err := bloomfilter.NewOptimal(100000, probCollide)
	if err != nil {
		panic(err)
	}

	fmt.Println(bf.M(), bf.M()/8/1000, bf.K())

	buff, err := bf.MarshalBinary()

	zipCompressor := &compress.ZipCompressor{}

	zipOut, err := zipCompressor.Compress(buff)
	if nil != err {
		panic(err)
	}

	fmt.Println(len(zipOut) / 1000 / 1000)

	fmt.Println(bf.HashString("hello"))
}

func test2(t *testing.T) {
	a := []int32{1, 2, 3, 4}
	b := (*reflect.SliceHeader)(unsafe.Pointer(&a))
	b.Len = 4 * b.Len
	b.Cap = 4 * b.Cap

	c := *(*[]byte)(unsafe.Pointer(b))

	fmt.Println(c, len(c), cap(c))
}

func test3(t *testing.T) {
	bf, err := bloomfilter.NewOptimal(1000, probCollide)
	if err != nil {
		panic(err)
	}

	hashString := bf.HashString("hello world")
	fmt.Println(hashString)
	bf.AddWithHashs(hashString)

	assert.Equal(t, bf.ContainsWithHashs(hashString), true)

	bin, err := bf.MarshalBinaryZip()

	var bf2 *bloomfilter.Filter = &bloomfilter.Filter{}

	err = bf2.UnmarshalBinaryZip(bin)

	assert.Equal(t, bf2.ContainsWithHashs(hashString), true)

	assert.Equal(t, bf.M(), bf2.M())

	assert.Equal(t, bf.K(), bf2.K())
}

func TestBloomFilter(t *testing.T) {
	//test1(t)
	//test2(t)
	test3(t)
}
