package compress

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"io"
	"io/ioutil"
)

type CompressorI interface {
	Compress(in []byte) ([]byte, error)
	Clone() CompressorI
}

type DecompressorI interface {
	Decompress(in []byte) ([]byte, error)
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
		if err != nil {
			return nil, err
		}
	} else {
		this.zipReader.(zlib.Resetter).Reset(&this.zipBuff, nil)
	}

	out, err := ioutil.ReadAll(this.zipReader)
	this.zipReader.Close()
	if err != nil {
		if err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, err
		}
	}

	return out, nil
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
		var err error
		this.zipReader, err = gzip.NewReader(&this.zipBuff)
		if nil != err {
			panic(err.Error())
			return nil, err
		}
	} else {
		this.zipReader.Reset(&this.zipBuff)
	}

	out, err := ioutil.ReadAll(this.zipReader)
	this.zipReader.Close()

	if err != nil {
		if err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, err
		}
	}

	return out, nil
}
