package net

import (
	"github.com/sniperHW/flyfish/net/pb"
)

func isPow2(size int) bool {
	return (size & (size - 1)) == 0
}

func sizeofPow2(size int) int {
	if isPow2(size) {
		return size
	}
	size = size - 1
	size = size | (size >> 1)
	size = size | (size >> 2)
	size = size | (size >> 4)
	size = size | (size >> 8)
	size = size | (size >> 16)
	return size + 1
}

type InboundProcessor struct {
	buffer  []byte
	w       int
	r       int
	pbSpace *pb.Namespace
	unpack  func(*pb.Namespace, []byte, int, int) (interface{}, int, error)
}
