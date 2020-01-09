package codec

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"io"
	"io/ioutil"
)

type CompressorI interface {
	Compress(in []byte) ([]byte, error)
}

type UnCompressorI interface {
	UnCompress(in []byte) ([]byte, error)
	ResetBuffer()
}

type ZipCompressor struct {
	zipBuff   bytes.Buffer
	zipWriter *zlib.Writer
}

func (this *ZipCompressor) Compress(in []byte) ([]byte, error) {
	if nil == this.zipWriter {
		this.zipWriter = zlib.NewWriter(&this.zipBuff)
	} else {
		this.zipBuff.Reset()
		this.zipWriter.Reset(&this.zipBuff)
	}

	this.zipWriter.Write(in)
	this.zipWriter.Flush()

	out := this.zipBuff.Bytes()

	return out, nil
}

type ZipUnCompressor struct {
	zipBuff bytes.Buffer
}

func (this *ZipUnCompressor) ResetBuffer() {
	this.zipBuff = bytes.Buffer{}
}

func (this *ZipUnCompressor) UnCompress(in []byte) ([]byte, error) {
	var err error
	var out []byte
	this.zipBuff.Reset()
	this.zipBuff.Write(in)
	var r io.ReadCloser
	r, err = zlib.NewReader(&this.zipBuff)
	if err != nil {
		return nil, err
	}

	out, err = ioutil.ReadAll(r)
	r.Close()
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

func (this *GZipCompressor) Compress(in []byte) ([]byte, error) {
	if nil == this.zipWriter {
		this.zipWriter = gzip.NewWriter(&this.zipBuff)
	} else {
		this.zipBuff.Reset()
		this.zipWriter.Reset(&this.zipBuff)
	}

	this.zipWriter.Write(in)
	this.zipWriter.Flush()

	out := this.zipBuff.Bytes()

	return out, nil
}

type GZipUnCompressor struct {
	zipBuff   bytes.Buffer
	zipReader *gzip.Reader
}

func (this *GZipUnCompressor) ResetBuffer() {
	this.zipBuff = bytes.Buffer{}
}

func (this *GZipUnCompressor) UnCompress(in []byte) ([]byte, error) {

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

	if err != nil {
		if err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, err
		}
	}

	return out, nil
}
