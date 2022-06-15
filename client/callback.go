package client

import (
	"github.com/sniperHW/flyfish/errcode"
)

type StatusResult struct {
	ErrCode errcode.Error
	Table   string
	Key     string
	ErrStr  string
	unikey  string
}

type SliceResult struct {
	ErrCode errcode.Error
	Table   string
	Key     string
	Version *int64
	Fields  map[string]*Field
	unikey  string
}

const (
	cb_status = 1
	cb_slice  = 2
)

type callback struct {
	tt      int32
	cb      interface{}
	sync    bool
	emmited int32
}

func splitUniKey(unikey string) (table string, key string) {
	i := -1
	for k, v := range unikey {
		if v == 58 {
			i = k
			break
		}
	}

	if i >= 0 {
		table = unikey[:i]
		key = unikey[i+1:]
	}

	return
}

func (this *callback) onError(unikey string, errCode errcode.Error) {
	if this.tt == cb_status {
		table, key := splitUniKey(unikey)
		this.cb.(func(*StatusResult))(&StatusResult{
			Key:     key,
			Table:   table,
			unikey:  unikey,
			ErrCode: errCode,
		})
	} else if this.tt == cb_slice {
		table, key := splitUniKey(unikey)
		this.cb.(func(*SliceResult))(&SliceResult{
			Key:     key,
			Table:   table,
			unikey:  unikey,
			ErrCode: errCode,
		})
	}
}

func (this *callback) onResult(unikey string, r interface{}) {
	if this.tt == cb_status {
		table, key := splitUniKey(unikey)
		ret := r.(*StatusResult)
		ret.Key = key
		ret.Table = table
		ret.unikey = unikey
		this.cb.(func(*StatusResult))(ret)
	} else if this.tt == cb_slice {
		table, key := splitUniKey(unikey)
		ret := r.(*SliceResult)
		ret.Key = key
		ret.Table = table
		ret.unikey = unikey
		this.cb.(func(*SliceResult))(ret)
	}
}
