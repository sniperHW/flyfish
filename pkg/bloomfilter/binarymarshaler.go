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
	//"bytes"
	"crypto/sha512"
	"encoding/binary"
	"github.com/sniperHW/flyfish/pkg/compress"
	"reflect"
	"unsafe"
)

// conforms to encoding.BinaryMarshaler

// marshalled binary layout (Little Endian):
//
//	 k	1 uint64
//	 n	1 uint64
//	 m	1 uint64
//	 keys	[k]uint64
//	 bits	[(m+63)/64]uint64
//	 hash	sha384 (384 bits == 48 bytes)
//
//	 size = (3 + k + (m+63)/64) * 8 bytes
//

func (f *Filter) MarshalBinary() (buf []byte) {
	buf = make([]byte, (9+len(f.keys)+len(f.bits))*8)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], f.K())
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.n)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.m)
	offset += 8
	keys := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  len(f.keys) * 8,
		Cap:  cap(f.keys) * 8,
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&f.keys)).Data,
	}))
	copy(buf[offset:], keys)
	offset += len(keys)

	bits := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  len(f.bits) * 8,
		Cap:  cap(f.bits) * 8,
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&f.bits)).Data,
	}))
	copy(buf[offset:], bits)
	offset += len(bits)

	hash := sha512.Sum384(buf[:offset])
	for _, v := range hash {
		buf[offset] = v
		offset++
	}
	return
}

// MarshalBinary converts a Filter into []bytes
func (f *Filter) MarshalBinaryZip() (buf []byte, err error) {
	buf = make([]byte, (9+len(f.keys)+len(f.bits))*8)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], f.K())
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.n)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], f.m)
	offset += 8

	keys := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  len(f.keys) * 8,
		Cap:  cap(f.keys) * 8,
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&f.keys)).Data,
	}))
	copy(buf[offset:], keys)

	offset += len(keys)
	zipCompressor := &compress.ZipCompressor{}

	var zipOut []byte

	zipOut, err = zipCompressor.Compress(*(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  len(f.bits) * 8,
		Cap:  cap(f.bits) * 8,
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&f.bits)).Data,
	})))

	if nil != err {
		return
	}

	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(zipOut)))
	offset += 8

	copy(buf[offset:], zipOut)
	offset += len(zipOut)

	hash := sha512.Sum384(buf[:offset])
	for _, v := range hash {
		buf[offset] = v
		offset++
	}

	buf = buf[:offset]
	return
}
