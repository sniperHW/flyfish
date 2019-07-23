package codec

import (
	"bytes"
	"compress/zlib"
	//"fmt"
	"io"
	"io/ioutil"
)

type CompressorI interface {
	Compress(in []byte) ([]byte, error)
}

type UnCompressorI interface {
	UnCompress(in []byte) ([]byte, error)
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

	//fmt.Println(len(in), len(out))

	return out, nil
}

type ZipUnCompressor struct {
	zipBuff bytes.Buffer
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
