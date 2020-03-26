package asyn

import (
	"context"
	"fmt"
	"time"
)

var (
	ErrTimeout = fmt.Errorf("timeout")
)

type Future struct {
	channel chan interface{}
	ret     []interface{}
	c       int
	cancel  context.CancelFunc
}

/*
* 等待所有闭包调用返回
 */

func (this *Future) Wait(timeout ...time.Duration) ([]interface{}, error) {
	defer this.cancel()
	if len(timeout) > 0 {

		deadline := time.Now().Add(timeout[0])
		for {
			now := time.Now()
			if now.Equal(deadline) || now.After(deadline) {
				this.cancel()
				return nil, ErrTimeout
			}

			remain := deadline.Sub(now)
			select {
			case ret := <-this.channel: //拿到锁
				this.c++
				idx := ret.([2]interface{})[0].(int)
				this.ret[idx] = ret.([2]interface{})[1]
				if this.c == len(this.ret) {
					//只有接收到所有结果才返回
					return this.ret, nil
				}
			case <-time.After(remain):
				return nil, ErrTimeout
			}
		}
	} else {
		for {
			ret := <-this.channel
			this.c++
			idx := ret.([2]interface{})[0].(int)
			this.ret[idx] = ret.([2]interface{})[1]
			if this.c == len(this.ret) {
				//只有接收到所有结果才返回
				return this.ret, nil
			}
		}
	}
	return nil, nil
}

/*
*  等待任意一个闭包调用返回
 */
func (this *Future) WaitAny(timeout ...time.Duration) (interface{}, error) {
	defer this.cancel()
	if len(timeout) > 0 {
		select {
		case ret := <-this.channel: //拿到锁
			return ret.([2]interface{})[1], nil
		case <-time.After(timeout[0]):
			return nil, ErrTimeout
		}
	} else {
		//接收到任意结果立即返回
		ret := <-this.channel
		return ret.([2]interface{})[1], nil
	}
}

/*
*  并行执行多个闭包(每个闭包在单独的goroutine上下文执行)
*  返回一个future,可以在将来的任何时刻等待闭包执行结果
 */
func Paralell(funcs ...func(ctx context.Context) interface{}) *Future {
	if 0 == len(funcs) {
		return nil
	}
	var ctx context.Context
	future := &Future{}
	future.channel = make(chan interface{}, len(funcs))
	future.ret = make([]interface{}, len(funcs))
	ctx, future.cancel = context.WithCancel(context.Background())
	for i := 0; i < len(funcs); i++ {
		go func(index int) {
			ret := [2]interface{}{nil, nil}
			ret[0] = index
			ret[1] = funcs[index](ctx)
			future.channel <- ret
		}(i)
	}

	return future
}
