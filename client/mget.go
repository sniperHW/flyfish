package client


import (
	"github.com/sniperHW/kendynet"
	"flyfish/errcode"
	"runtime"
)


type MGetCmd struct {
	c          *Client
	cmds       map[string]*SliceCmd
	rows       []*Row
	cb         func(*MutiResult)
	respCount  int
}


func mGetPcall(cb func(*MutiResult),ret *MutiResult) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			kendynet.Errorf("%v: %s\n", r, buf[:l])
		}			
	}()
	cb(ret)		
}

func (this *Client) mGetDoCallBack(cb func(*MutiResult),ret *MutiResult) {
	if nil != this.callbackQueue {
		this.callbackQueue.Post(func(){
			cb(ret)
		})
	} else {
		mGetPcall(cb,ret)
	}	
}

func (this *MGetCmd) Exec(cb func(*MutiResult)) {
	this.cb = cb
	for k,v := range(this.cmds) {
		key := k
		v.Exec(func(ret *SliceResult){
			this.c.mGetQueue.PostNoWait(func() {
				if nil == this.cb {
					return
				}
				if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_NOTFOUND {

					this.respCount++
					if nil == this.rows {
						this.rows = []*Row{}
					}

					if ret.ErrCode == errcode.ERR_OK { 
						row := &Row {
							Key : key,
							Version : ret.Version,
							Fields : ret.Fields,
						}
						this.rows = append(this.rows,row)
					} else {
						row := &Row {
							Key : key,
						}
						this.rows = append(this.rows,row)						
					}

					if this.respCount == len(this.cmds) {
						cb := this.cb
						this.cb = nil
						this.c.mGetDoCallBack(cb,&MutiResult{
							ErrCode : errcode.ERR_OK,
							Rows : this.rows,
						})
					}

				} else {
					cb := this.cb
					this.cb = nil
					this.c.mGetDoCallBack(cb,&MutiResult{
						ErrCode : ret.ErrCode,
					})
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
		cmds : map[string]*SliceCmd{},
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



func (this *Client) MGetAll(table string,keys []string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}


	cmd := &MGetCmd{
		c      : this,
		cmds   : map[string]*SliceCmd{},
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
