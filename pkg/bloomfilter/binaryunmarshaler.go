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
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"github.com/sniperHW/flyfish/pkg/compress"
	"reflect"
	"unsafe"
)

func unmarshalBinaryHeader(buf []byte, offset *int) (k, n, m uint64, err error) {
	if len(buf[*offset:]) < 24 {
		return k, n, m, errors.New("not enough data")
	}

	k = binary.LittleEndian.Uint64(buf[*offset:])
	(*offset) += 8
	n = binary.LittleEndian.Uint64(buf[*offset:])
	(*offset) += 8
	m = binary.LittleEndian.Uint64(buf[*offset:])
	(*offset) += 8

	if k < KMin {
		return k, n, m, errK()
	}

	if m < MMin {
		return k, n, m, errM()
	}

	return k, n, m, err
}

func unmarshalBinaryKeys(buf []byte, k uint64, offset *int) (keys []uint64, err error) {
	if uint64(len(buf[*offset:])) < k*8 {
		return keys, errors.New("not enough data")
	}

	bb := buf[*offset:]

	kk := *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  int(k),
		Cap:  int(k),
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&bb)).Data,
	}))

	keys = make([]uint64, k)

	for i, v := range kk {
		keys[i] = v
	}

	(*offset) += int(k * 8)
	return keys, err
}

func checkBinaryHash(buf []byte, offset *int) (err error) {
	if len(buf[*offset:]) < sha512.Size384 {
		err = errors.New("not enough data")
		return err
	}

	expectedHash := buf[*offset : *offset+sha512.Size384]
	actualHash := sha512.Sum384(buf[:len(buf)-sha512.Size384])

	if !hmac.Equal(expectedHash, actualHash[:]) {
		debug("bloomfilter.UnmarshalBinary() sha384 hash failed:"+
			" actual %v  expected %v", actualHash, expectedHash)
		return errHash()
	}

	return nil
}

// UnmarshalBinary converts []bytes into a Filter
// conforms to encoding.BinaryUnmarshaler
func (f *Filter) UnmarshalBinary(data []byte) (err error) {
	var offset int
	var k uint64
	k, f.n, f.m, err = unmarshalBinaryHeader(data, &offset)
	if err != nil {
		return err
	}

	f.keys, err = unmarshalBinaryKeys(data, k, &offset)
	if err != nil {
		return err
	}

	lenBits := (f.m + 63) / 64

	if uint64(len(data[offset:])) < lenBits*8 {
		return errors.New("not enough data")
	}

	f.bits = make([]uint64, lenBits)

	bb := data[offset:]

	for i, v := range *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  int(lenBits),
		Cap:  int(lenBits),
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&bb)).Data,
	})) {
		f.bits[i] = v
	}

	offset += int(lenBits * 8)

	return checkBinaryHash(data, &offset)
}

func (f *Filter) UnmarshalBinaryZip(decompressor compress.DecompressorI, data []byte) (err error) {

	var offset int
	var k uint64
	k, f.n, f.m, err = unmarshalBinaryHeader(data, &offset)
	if err != nil {
		return err
	}

	f.keys, err = unmarshalBinaryKeys(data, k, &offset)
	if err != nil {
		return err
	}

	if len(data[offset:]) < 8 {
		return errors.New("not enough data")
	}

	lenZip := int(binary.LittleEndian.Uint64(data[offset:]))

	offset += 8

	if len(data[offset:]) < lenZip {
		return errors.New("not enough data")
	}

	unzipOut, err := decompressor.Decompress(data[offset : offset+lenZip])
	if nil != err {
		return err
	}

	lenBits := (f.m + 63) / 64

	f.bits = make([]uint64, lenBits)

	for i, v := range *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{
		Len:  int(lenBits),
		Cap:  int(lenBits),
		Data: (*reflect.SliceHeader)(unsafe.Pointer(&unzipOut)).Data,
	})) {
		f.bits[i] = v
	}

	offset += int(lenZip)

	return checkBinaryHash(data, &offset)
}
