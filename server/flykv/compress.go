package flykv

import (
	"github.com/sniperHW/flyfish/pkg/compress"
	"sync"
)

var compressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &compress.ZipCompressor{}
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
		return &compress.ZipDecompressor{}
	},
}

func getDecompressor() compress.DecompressorI {
	return decompressorPool.Get().(compress.DecompressorI)
}

func releaseDecompressor(c compress.DecompressorI) {
	decompressorPool.Put(c)
}