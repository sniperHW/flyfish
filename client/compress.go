package client

import (
	"github.com/sniperHW/flyfish/pkg/compress"
)

func getCompressor() compress.CompressorI {
	return &compress.ZipCompressor{}
}

func getDecompressor() compress.DecompressorI {
	return &compress.ZipDecompressor{}
}

func checkHeader(in []byte) (bool, int) {
	return getDecompressor().CheckHeader(in)
}
