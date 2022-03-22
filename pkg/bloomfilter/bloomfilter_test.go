// Package bloomfilter is face-meltingly fast, thread-safe,
// marshalable, unionable, probability- and
// optimal-size-calculating Bloom filter in go
//
// https://github.com/steakknife/bloomfilter
//
// Copyright © 2014, 2015, 2018 Barry Allard
//
// MIT license
//
package bloomfilter

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	//"math/rand"
	"testing"
	"time"
)

const (
	maxElements = 100000000
	probCollide = 0.01
)

func TestZip2(t *testing.T) {
	bf, err := NewOptimal(10000, probCollide)
	if err != nil {
		panic(err)
	}

	beg := time.Now()
	for i := 0; i < 16384; i++ {
		bf.Clone()
	}
	fmt.Println("use", time.Now().Sub(beg))
}

func TestZip(t *testing.T) {
	bf, err := NewOptimal(10000, probCollide)
	if err != nil {
		panic(err)
	}

	hashString := bf.HashString("hello world")
	fmt.Println(hashString)
	bf.AddWithHashs(hashString)

	assert.Equal(t, bf.ContainsWithHashs(hashString), true)

	beg := time.Now()

	bin, err := bf.MarshalBinaryZip()

	fmt.Println("use", time.Now().Sub(beg))

	var bf2 *Filter = &Filter{}

	err = bf2.UnmarshalBinaryZip(bin)

	assert.Nil(t, err)

	fmt.Println(bf.m, bf.n, bf2.m, bf2.n)

	assert.Equal(t, bf.M(), bf2.M())

	assert.Equal(t, bf.K(), bf2.K())

	for i := 0; i < len(bf.keys); i++ {
		assert.Equal(t, bf.keys[i], bf2.keys[i])
	}

	assert.Equal(t, len(bf.bits), len(bf2.bits))

	for i := 0; i < len(bf.bits); i++ {
		assert.Equal(t, bf.bits[i], bf2.bits[i])
	}

	assert.Equal(t, bf2.ContainsWithHashs(hashString), true)

	assert.Equal(t, bf.M(), bf2.M())

	assert.Equal(t, bf.K(), bf2.K())

	fmt.Println(bf.M() / 8 / 1024 / 1024)
}

func TestMarshal(t *testing.T) {
	bf, err := NewOptimal(10000, probCollide)
	if err != nil {
		panic(err)
	}

	hashString := bf.HashString("hello world")
	fmt.Println(hashString)
	bf.AddWithHashs(hashString)

	assert.Equal(t, bf.ContainsWithHashs(hashString), true)

	beg := time.Now()

	bin := bf.MarshalBinary()

	fmt.Println("use", time.Now().Sub(beg))

	var bf2 *Filter = &Filter{}

	err = bf2.UnmarshalBinary(bin)

	assert.Nil(t, err)

	fmt.Println(bf.m, bf.n, bf2.m, bf2.n)

	assert.Equal(t, bf.M(), bf2.M())

	assert.Equal(t, bf.K(), bf2.K())

	for i := 0; i < len(bf.keys); i++ {
		assert.Equal(t, bf.keys[i], bf2.keys[i])
	}

	assert.Equal(t, len(bf.bits), len(bf2.bits))

	for i := 0; i < len(bf.bits); i++ {
		assert.Equal(t, bf.bits[i], bf2.bits[i])
	}

	assert.Equal(t, bf2.ContainsWithHashs(hashString), true)

	assert.Equal(t, bf.M(), bf2.M())

	assert.Equal(t, bf.K(), bf2.K())

	fmt.Println(bf.M() / 8 / 1024 / 1024)
}

/*
// a read-only type that conforms to hash.Hash64, but only Sum64() works.
// It is set by writing the underlying value.
type hashableUint64 uint64

func (h hashableUint64) Write([]byte) (int, error) {
	panic("Unimplemented")
}

func (h hashableUint64) Sum([]byte) []byte {
	panic("Unimplemented")
}

func (h hashableUint64) Reset() {
	panic("Unimplemented")
}

func (h hashableUint64) BlockSize() int {
	panic("Unimplemented")
}

func (h hashableUint64) Size() int {
	panic("Unimplemented")
}

func (h hashableUint64) Sum64() uint64 {
	return uint64(h)
}

func hashableUint64Values() []hashableUint64 {
	return []hashableUint64{
		0,
		7,
		0x0c0ffee0,
		0xdeadbeef,
		0xffffffff,
	}
}

func hashableUint64NotValues() []hashableUint64 {
	return []hashableUint64{
		1,
		5,
		42,
		0xa5a5a5a5,
		0xfffffffe,
	}
}

func Test0(t *testing.T) {
	bf, _ := New(10000, 5)

	t.Log("Filled ratio before adds :", bf.PreciseFilledRatio())
	for _, x := range hashableUint64Values() {
		bf.Add(x)
	}
	t.Log("Filled ratio after adds :", bf.PreciseFilledRatio())

	// these may or may not be true
	for _, y := range hashableUint64Values() {
		if bf.Contains(y) {
			t.Log("value in set querties: may contain ", y)
		} else {
			t.Fatal("value in set queries: definitely does not contain ", y,
				", but it should")
		}
	}

	// these must all be false
	for _, z := range hashableUint64NotValues() {
		if bf.Contains(z) {
			t.Log("value not in set queries: may or may not contain ", z)
		} else {
			t.Log("value not in set queries: definitely does not contain ", z,
				" which is correct")
		}
	}
}

func BenchmarkAddX10kX5(b *testing.B) {
	b.StopTimer()
	bf, _ := New(10000, 5)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(hashableUint64(rand.Uint32()))
	}
}

func BenchmarkContains1kX10kX5(b *testing.B) {
	b.StopTimer()
	bf, _ := New(10000, 5)
	for i := 0; i < 1000; i++ {
		bf.Add(hashableUint64(rand.Uint32()))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(hashableUint64(rand.Uint32()))
	}
}

func BenchmarkContains100kX10BX20(b *testing.B) {
	b.StopTimer()
	bf, _ := New(10*1000*1000*1000, 20)
	for i := 0; i < 100*1000; i++ {
		bf.Add(hashableUint64(rand.Uint32()))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(hashableUint64(rand.Uint32()))
	}
}
*/
