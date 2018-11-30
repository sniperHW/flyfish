package asyn


/*
*  将同步接函数调用转换成基于回调的接口
*/ 

import(
	"github.com/sniperHW/kendynet/event"
	"reflect"
)

var routinePool_ * routinePool

type wrapFunc func(callback func([]interface{}),args ...interface{})

func AsynWrap(queue *event.EventQueue,oriFunc interface{}) wrapFunc {

	if nil == queue {
		return nil
	}

	oriF := reflect.ValueOf(oriFunc)

	if oriF.Kind() != reflect.Func {
		return nil
	}

	return func(callback func([]interface{}),args ...interface{}){
		f := func () {
			in := []reflect.Value{}
			for _,v := range(args) {
				in = append(in,reflect.ValueOf(v))
			}
			out := oriF.Call(in) 
			ret := []interface{}{}
			for _,v := range(out) {
				ret = append(ret,v.Interface())
			}
			if nil != callback {
 				queue.Post(callback,ret...)
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

