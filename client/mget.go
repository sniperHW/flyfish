package client

import (
	"github.com/sniperHW/flyfish/errcode"
)

type MGetCmd struct {
	c         *Client
	cmds      map[string]*SliceCmd
	rows      []*Row
	cb        func(*MutiResult)
	respCount int
}

func (this *MGetCmd) AsyncExec(cb func(*MutiResult)) {
	this.cb = cb
	for k, v := range this.cmds {
		key := k
		v.AsyncExec(func(ret *SliceResult) {
			this.c.mGetQueue.PostNoWait(func() {
				if nil == this.cb {
					return
				}
				if ret.ErrCode == errcode.ERR_OK || ret.ErrCode == errcode.ERR_RECORD_EXIST {

					this.respCount++
					if nil == this.rows {
						this.rows = []*Row{}
					}

					if ret.ErrCode == errcode.ERR_OK {
						row := &Row{
							Key:     key,
							Version: ret.Version,
							Fields:  ret.Fields,
						}
						this.rows = append(this.rows, row)
					} else {
						row := &Row{
							Key: key,
						}
						this.rows = append(this.rows, row)
					}

					if this.respCount == len(this.cmds) {
						cb := callback{
							tt: cb_muti,
							cb: this.cb,
						}
						this.cb = nil
						this.c.doCallBack(cb, &MutiResult{
							ErrCode: errcode.ERR_OK,
							Rows:    this.rows,
						})
					}

				} else {
					cb := callback{
						tt: cb_muti,
						cb: this.cb,
					}
					this.cb = nil
					this.c.doCallBack(cb, ret.ErrCode)
				}
			})
		})
	}
}

func (this *MGetCmd) Exec() *MutiResult {
	respChan := make(chan *MutiResult)
	this.AsyncExec(func(r *MutiResult) {
		respChan <- r
	})
	return <-respChan
}

func (this *Client) MGet(table string, keys []string, fields ...string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}

	cmd := &MGetCmd{
		c:    this,
		cmds: map[string]*SliceCmd{},
	}

	for _, key := range keys {
		c := this.Get(table, key, fields...)
		if nil == c {
			return nil
		}
		cmd.cmds[key] = c
	}

	return cmd
}

func (this *Client) MGetAll(table string, keys ...string) *MGetCmd {
	l := len(keys)
	if l == 0 {
		return nil
	}

	if l > 200 {
		return nil
	}

	cmd := &MGetCmd{
		c:    this,
		cmds: map[string]*SliceCmd{},
	}

	for _, key := range keys {
		c := this.GetAll(table, key)
		if nil == c {
			return nil
		}
		cmd.cmds[key] = c
	}

	return cmd
}
