package compress

import (
	"bytes"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"io"
	"io/ioutil"
)

const (
	gzipID1     = 0x1f
	gzipID2     = 0x8b
	gzipDeflate = 8
	zlibDeflate = 8
)

type CompressorI interface {
	Compress(in []byte) ([]byte, error)
	Clone() CompressorI
}

type DecompressorI interface {
	Decompress(in []byte) ([]byte, error)
	CheckHeader(in []byte) (bool, int)
	Clone() DecompressorI
}

type ZipCompressor struct {
	zipBuff   bytes.Buffer
	zipWriter *zlib.Writer
}

func (this *ZipCompressor) Clone() CompressorI {
	return &ZipCompressor{}
}

func (this *ZipCompressor) Compress(in []byte) ([]byte, error) {
	if nil == this.zipWriter {
		this.zipWriter = zlib.NewWriter(&this.zipBuff)
	} else {
		this.zipBuff.Reset()
		this.zipWriter.Reset(&this.zipBuff)
	}

	var err error

	_, err = this.zipWriter.Write(in)
	if nil != err {
		return nil, err
	}

	err = this.zipWriter.Flush()
	if nil != err {
		return nil, err
	}

	out := this.zipBuff.Bytes()

	return out, nil
}

type ZipDecompressor struct {
	zipBuff   bytes.Buffer
	zipReader io.ReadCloser
}

func (this *ZipDecompressor) CheckHeader(in []byte) (bool, int) {
	if len(in) < 2 {
		return false, 0
	} else {
		h := uint(in[0])<<8 | uint(in[1])
		if (in[0]&0x0f != zlibDeflate) || (h%31 != 0) {
			return false, 0
		} else {
			return true, 2
		}
	}
}

func (this *ZipDecompressor) Clone() DecompressorI {
	return &ZipDecompressor{}
}

func (this *ZipDecompressor) Decompress(in []byte) ([]byte, error) {

	var err error

	this.zipBuff.Reset()

	_, err = this.zipBuff.Write(in)
	if nil != err {
		return nil, err
	}

	if nil == this.zipReader {
		this.zipReader, err = zlib.NewReader(&this.zipBuff)
	} else {
		err = this.zipReader.(zlib.Resetter).Reset(&this.zipBuff, nil)

	}

	if err != nil {
		return nil, err
	}

	out, err := ioutil.ReadAll(this.zipReader)
	this.zipReader.Close()

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return out, nil
	} else {
		return nil, err
	}
}

type GZipCompressor struct {
	zipBuff   bytes.Buffer
	zipWriter *gzip.Writer
}

func (this *GZipCompressor) Clone() CompressorI {
	return &GZipCompressor{}
}

func (this *GZipCompressor) Compress(in []byte) ([]byte, error) {
	if nil == this.zipWriter {
		this.zipWriter = gzip.NewWriter(&this.zipBuff)
	} else {
		this.zipBuff.Reset()
		this.zipWriter.Reset(&this.zipBuff)
	}

	var err error

	_, err = this.zipWriter.Write(in)

	if nil != err {
		return nil, err
	}

	err = this.zipWriter.Flush()

	if nil != err {
		return nil, err
	}

	out := this.zipBuff.Bytes()

	return out, nil
}

type GZipDecompressor struct {
	zipBuff   bytes.Buffer
	zipReader *gzip.Reader
}

func (this *GZipDecompressor) CheckHeader(in []byte) (bool, int) {
	if len(in) < 10 {
		return false, 0
	} else {
		if in[0] != gzipID1 || in[1] != gzipID2 || in[2] != gzipDeflate {
			return false, 0
		} else {
			return true, 10
		}
	}
}

func (this *GZipDecompressor) Clone() DecompressorI {
	return &GZipDecompressor{}
}

func (this *GZipDecompressor) Decompress(in []byte) ([]byte, error) {

	this.zipBuff.Reset()
	_, err := this.zipBuff.Write(in)

	if nil != err {
		return nil, err
	}

	if nil == this.zipReader {
		this.zipReader, err = gzip.NewReader(&this.zipBuff)
	} else {
		err = this.zipReader.Reset(&this.zipBuff)
	}

	if nil != err {
		return nil, err
	}

	out, err := ioutil.ReadAll(this.zipReader)
	this.zipReader.Close()

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return out, nil
	} else {
		return nil, err
	}
}
