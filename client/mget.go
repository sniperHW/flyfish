package client

import (
	"github.com/sniperHW/kendynet"
	"flyfish/errcode"
	"runtime"
)

type ResultSet struct {
	ErrCode  int32
	Results  map[string]*Result	
}

type MGetCallBack func(*ResultSet)

type MGetCmd struct {
	c          *Client
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
			this.c.mGetQueue.PostNoWait(func() {
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
						this.c.mGetDoCallBack(callback,&this.resultSet)
					}

				} else {
					callback := this.callback
					this.callback = nil
					this.resultSet.ErrCode = r.ErrCode
					this.c.mGetDoCallBack(callback,&this.resultSet)
				}
			})
		})
	}
}


func (this *Client) MGet(table string,keys []string,fields ...string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}

	cmd := &MGetCmd{
		c    : this,
		cmds : map[string]*Cmd{},
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
func (this *Conn) MGetAll(table string,keys []string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}


	cmd := &MGetCmd{
		conn   : this,
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
}*/
