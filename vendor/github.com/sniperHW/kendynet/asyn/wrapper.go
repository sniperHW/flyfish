package asyn

/*
*  将同步接函数调用转换成基于回调的接口
 */

import (
	"github.com/sniperHW/kendynet/event"
	"reflect"
)

var routinePool_ *routinePool

type wrapFunc func(callback func([]interface{}), args ...interface{})

func AsynWrap(queue *event.EventQueue, fn interface{}) wrapFunc {

	if nil == queue {
		return nil
	}

	oriF := reflect.ValueOf(fn)

	if oriF.Kind() != reflect.Func {
		return nil
	}

	fnType := reflect.TypeOf(fn)

	return func(callback func([]interface{}), args ...interface{}) {
		f := func() {
			var in []reflect.Value
			numIn := fnType.NumIn()
			if numIn > 0 {
				in = make([]reflect.Value, numIn)
				for i := 0; i < numIn; i++ {
					if i >= len(args) || args[i] == nil {
						in[i] = reflect.Zero(fnType.In(i))
					} else {
						in[i] = reflect.ValueOf(args[i])
					}
				}
			}

			out := oriF.Call(in)

			if len(out) > 0 {
				ret := make([]interface{}, len(out))
				for i, v := range out {
					ret[i] = v.Interface()
				}
				if nil != callback {
					queue.PostNoWait(callback, ret...)
				}
			} else {
				if nil != callback {
					queue.PostNoWait(callback)
				}
			}
		}

		if nil == routinePool_ {
			go f()
		} else {
			//设置了go程池，交给go程池执行
			routinePool_.AddTask(f)
		}
	}
}

func SetRoutinePool(pool *routinePool) {
	routinePool_ = pool
}
