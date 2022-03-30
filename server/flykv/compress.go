package flykv

import (
	"github.com/sniperHW/flyfish/pkg/compress"
)

func getCompressor() compress.CompressorI {
	return compress.GetGZipCompressor()
}

func releaseCompressor(c compress.CompressorI) {
	compress.PutGZipCompressor(c)
}

func getDecompressor() compress.DecompressorI {
	return compress.GetGZipDecompressor()
}

func releaseDecompressor(c compress.DecompressorI) {
	compress.PutGZipDecompressor(c)
}
