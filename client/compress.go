package client

import (
	"github.com/sniperHW/flyfish/pkg/compress"
	"sync"
)

func makeCompressor() compress.CompressorI {
	return &compress.ZipCompressor{}
}

func makeDecompressor() compress.DecompressorI {
	return &compress.ZipDecompressor{}
}

var compressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return makeCompressor()
	},
}

func getCompressor() compress.CompressorI {
	return compressorPool.Get().(compress.CompressorI)
}

func releaseCompressor(c compress.CompressorI) {
	compressorPool.Put(c)
}

var decompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return makeDecompressor()
	},
}

func getDecompressor() compress.DecompressorI {
	return decompressorPool.Get().(compress.DecompressorI)
}

func releaseDecompressor(c compress.DecompressorI) {
	decompressorPool.Put(c)
}

func checkHeader(in []byte) (bool, int) {
	return makeDecompressor().CheckHeader(in)
}
