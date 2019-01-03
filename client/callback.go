package client

import (
//protocol "flyfish/proto"
//"github.com/golang/protobuf/proto"
//"github.com/sniperHW/kendynet/util"
//"flyfish/codec"
//"github.com/sniperHW/kendynet"
//"time"
//"sync/atomic"
//"flyfish/errcode"
)

type StatusResult struct {
	ErrCode int32
	Version int64
}

type SliceResult struct {
	ErrCode int32
	Version int64
	Fields  map[string]*Field
}

type Row struct {
	Key     string
	Version int64
	Fields  map[string]*Field
}

type MutiResult struct {
	ErrCode int32
	Rows    []*Row
}

const (
	cb_status = 1
	cb_slice  = 2
	cb_muti   = 3
)

type callback struct {
	tt int32
	cb interface{}
}

func (this *callback) onError(errCode int32) {
	if this.tt == cb_status {
		this.cb.(func(*StatusResult))(&StatusResult{
			ErrCode: errCode,
		})
	} else if this.tt == cb_slice {
		this.cb.(func(*SliceResult))(&SliceResult{
			ErrCode: errCode,
		})
	} else if this.tt == cb_muti {
		this.cb.(func(*MutiResult))(&MutiResult{
			ErrCode: errCode,
		})
	} else {
		panic("invaild cb_type")
	}
}

func (this *callback) onResult(r interface{}) {
	if this.tt == cb_status {
		this.cb.(func(*StatusResult))(r.(*StatusResult))
	} else if this.tt == cb_slice {
		this.cb.(func(*SliceResult))(r.(*SliceResult))
	} else if this.tt == cb_muti {
		this.cb.(func(*MutiResult))(r.(*MutiResult))
	} else {
		panic("invaild cb_type")
	}
}
