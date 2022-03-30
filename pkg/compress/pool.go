package compress

import (
	"sync"
)

var zlibCompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &ZipCompressor{}
	},
}

func GetZlibCompressor() CompressorI {
	return zlibCompressorPool.Get().(CompressorI)
}

func PutZlibCompressor(c CompressorI) {
	zlibCompressorPool.Put(c)
}

var zlibDecompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &ZipDecompressor{}
	},
}

func GetZlibDecompressor() DecompressorI {
	return zlibDecompressorPool.Get().(DecompressorI)
}

func PutZlibDecompressor(c DecompressorI) {
	zlibDecompressorPool.Put(c)
}

var gzipCompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &GZipCompressor{}
	},
}

func GetGZipCompressor() CompressorI {
	return gzipCompressorPool.Get().(CompressorI)
}

func PutGZipCompressor(c CompressorI) {
	gzipCompressorPool.Put(c)
}

var gzipDecompressorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &GZipDecompressor{}
	},
}

func GetGZipDecompressor() DecompressorI {
	return gzipDecompressorPool.Get().(DecompressorI)
}

func PutGZipDecompressor(c DecompressorI) {
	gzipDecompressorPool.Put(c)
}
