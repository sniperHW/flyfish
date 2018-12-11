package client

import (
	//protocol "flyfish/proto"
	//"github.com/golang/protobuf/proto"
	//"github.com/sniperHW/kendynet/util"
	//"flyfish/codec"
	"github.com/sniperHW/kendynet"
	//"time"
	//"sync/atomic"
	"flyfish/errcode"
	"runtime"
	//"fmt"
)


type ResultSet struct {
	ErrCode  int32
	Results  map[string]*Result	
}

type MGetCallBack func(*ResultSet)

type MGetCmd struct {
	client     *Client
	cmds       map[string]*Cmd
	resultSet  ResultSet
	callback   MGetCallBack
	respCount  int
}


func mGetPcall(cb MGetCallBack,r *ResultSet) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			kendynet.Errorf("%v: %s\n", r, buf[:l])
		}			
	}()
	cb(r)		
}

func (this *Client) mGetDoCallBack(cb MGetCallBack,r *ResultSet) {
	if nil != this.callbackQueue {
		this.callbackQueue.Post(func(){
			cb(r)
		})
	} else {
		mGetPcall(cb,r)
	}	
}

func (this *MGetCmd) Exec(cb MGetCallBack) {
	this.callback = cb
	for k,v := range(this.cmds) {
		key := k
		v.Exec(func(r *Result){

			if nil == this.callback {
				return
			}
			if r.ErrCode == errcode.ERR_OK || r.ErrCode == errcode.ERR_NOTFOUND {

				this.respCount++
				if nil == this.resultSet.Results {
					this.resultSet.Results = map[string]*Result{}
				}
				if r.ErrCode == errcode.ERR_OK { 
					this.resultSet.Results[key] = r
				} else {
					this.resultSet.Results[key] = nil
				}
				if this.respCount == len(this.cmds) {
					callback := this.callback
					this.callback = nil
					this.client.mGetDoCallBack(callback,&this.resultSet)
				}

			} else {
				callback := this.callback
				this.callback = nil
				this.resultSet.ErrCode = r.ErrCode
				this.client.mGetDoCallBack(callback,&this.resultSet)
			}
		})
	}
}


/*
*   任意一条指令返回失败都会导致整个mget失败
*/
func (this *Client) MGet(table string,keys []string,fields ...string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}


	cmd := &MGetCmd{
		client : this,
		cmds   : map[string]*Cmd{},
	}

	for _,key := range(keys) {
		c := this.Get(table,key,fields...)
		if nil == c {
			return nil
		}
		cmd.cmds[key] = c
	}

	return cmd
}


/*
*   任意一条指令返回失败都会导致整个mget失败
*/
func (this *Client) MGetAll(table string,keys []string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}


	cmd := &MGetCmd{
		client : this,
		cmds   : map[string]*Cmd{},
	}

	for _,key := range(keys) {
		c := this.GetAll(table,key)
		if nil == c {
			return nil
		}
		cmd.cmds[key] = c
	}

	return cmd
}
