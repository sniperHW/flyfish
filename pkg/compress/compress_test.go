package compress

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompress(t *testing.T) {

	s := strings.Repeat("a", 4096)

	ss := []byte(s)

	func() {

		zipCompressor := &ZipCompressor{}

		zipOut, err := zipCompressor.Compress(ss)
		if nil != err {
			t.Fatal(err)
		}

		fmt.Println("zip len", len(zipOut))

		zipDecompressor := &ZipDecompressor{}

		if ok, _ := zipDecompressor.CheckHeader(zipOut); !ok {
			t.Fatal("CheckHeader")
		}

		unzipOut, err := zipDecompressor.Decompress(zipOut)

		if nil != err {
			t.Fatal(err)
		}

		if string(unzipOut) != s {
			t.Fatal(len(unzipOut))
		}

		unzipOut, err = zipDecompressor.Decompress(zipOut)

		if nil != err {
			t.Fatal(err)
		}

		if string(unzipOut) != s {
			t.Fatal(unzipOut)
		}

		unzipOut, err = zipDecompressor.Decompress(zipOut)

		if nil != err {
			t.Fatal(err)
		}

		if string(unzipOut) != s {
			t.Fatal(unzipOut)
		}

	}()

	func() {

		gzipCompressor := &GZipCompressor{}

		zipOut, err := gzipCompressor.Compress(ss)
		if nil != err {
			t.Fatal(err)
		}

		fmt.Println("gzip len", len(zipOut))

		gzipDecompressor := &GZipDecompressor{}

		if ok, _ := gzipDecompressor.CheckHeader(zipOut); !ok {
			t.Fatal("CheckHeader")
		}

		unzipOut, err := gzipDecompressor.Decompress(zipOut)

		if nil != err {
			t.Fatal(err)
		}

		if string(unzipOut) != s {
			t.Fatal(unzipOut)
		}

		unzipOut, err = gzipDecompressor.Decompress(zipOut)

		if nil != err {
			t.Fatal(err)
		}

		if string(unzipOut) != s {
			t.Fatal(unzipOut)
		}

	}()

}

func BenchmarkZip(b *testing.B) {
	numLoops := b.N
	s := []byte(strings.Repeat("a", 4096))
	zipCompressor := &ZipCompressor{}
	zipDecompressor := &ZipDecompressor{}

	for i := 0; i < numLoops; i++ {
		zipOut, err := zipCompressor.Compress(s)
		if nil != err {
			b.Fatal(err)
		}
		_, err = zipDecompressor.Decompress(zipOut)
		if nil != err {
			b.Fatal(err)
		}
	}
}

func BenchmarkGZip(b *testing.B) {
	numLoops := b.N
	s := []byte(strings.Repeat("a", 4096))
	zipCompressor := &GZipCompressor{}
	zipDecompressor := &GZipDecompressor{}

	for i := 0; i < numLoops; i++ {
		zipOut, err := zipCompressor.Compress(s)
		if nil != err {
			b.Fatal(err)
		}
		_, err = zipDecompressor.Decompress(zipOut)
		if nil != err {
			b.Fatal(err)
		}
	}
}
