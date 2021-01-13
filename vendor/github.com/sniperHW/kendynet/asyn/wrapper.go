package asyn

/*
*  将同步接函数调用转换成基于回调的接口
 */

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
	"reflect"
)

var routinePool_ *routinePool

type wrapFunc func(callback interface{}, args ...interface{})

func AsynWrap(queue *event.EventQueue, fn interface{}) wrapFunc {

	if nil == queue {
		return nil
	}

	oriF := reflect.ValueOf(fn)

	if oriF.Kind() != reflect.Func {
		return nil
	}

	return func(callback interface{}, args ...interface{}) {
		f := func() {
			out, err := util.ProtectCall(fn, args...)
			if err != nil {
				logger := kendynet.GetLogger()
				if logger != nil {
					logger.Errorln(err)
				}
				return
			}

			if len(out) > 0 {
				if nil != callback {
					queue.PostNoWait(callback, out...)
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
