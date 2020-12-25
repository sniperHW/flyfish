package util

import (
	"reflect"
)

/*
 *  如果设置了before则只有before返回true才会继续执行被hook函数
 *  如果before返回false,则将before构造的返回值作为被hook函数的返回值
 *  如果before返回false,因为被hook函数不会执行，因此，after也不会执行
 */

func Hook1(fn interface{}, before func() (bool, []interface{}), after func()) interface{} {
	fnType := reflect.TypeOf(fn)
	hookCB := reflect.MakeFunc(fnType, func(in []reflect.Value) []reflect.Value {

		var out []reflect.Value

		if nil != before {
			ok, ret := before()
			if !ok {
				if fnType.NumOut() > 0 {
					for i := 0; i < fnType.NumOut(); i++ {
						if i >= len(ret) || ret[i] == nil {
							out = append(out, reflect.Zero(fnType.In(i)))
						} else {
							out = append(out, reflect.ValueOf(ret[i]))
						}
					}
				}
				return out
			}
		}

		if nil != after {
			defer after()
		}

		if fnType.IsVariadic() {
			out = reflect.ValueOf(fn).CallSlice(in)
		} else {
			out = reflect.ValueOf(fn).Call(in)
		}

		return out
	})

	return hookCB.Interface()
}

/*
 * 先执行before,再执行被hook的函数，最后执行after
 * 如果before/after接受参数，则参数必须与被hook的函数一致
 */
func Hook2(fn interface{}, before interface{}, after interface{}) interface{} {
	fnType := reflect.TypeOf(fn)

	hookCB := reflect.MakeFunc(fnType, func(in []reflect.Value) []reflect.Value {

		if nil != before {
			if reflect.TypeOf(before).NumIn() == 0 {
				reflect.ValueOf(before).Call(nil)
			} else {
				if fnType.IsVariadic() {
					reflect.ValueOf(before).CallSlice(in)
				} else {
					reflect.ValueOf(before).Call(in)
				}
			}
		}

		var out []reflect.Value

		if nil != after {
			defer func() {
				if reflect.TypeOf(after).NumIn() == 0 {
					reflect.ValueOf(after).Call(nil)
				} else {
					if fnType.IsVariadic() {
						reflect.ValueOf(after).CallSlice(in)
					} else {
						reflect.ValueOf(after).Call(in)
					}
				}
			}()
		}

		if fnType.IsVariadic() {
			out = reflect.ValueOf(fn).CallSlice(in)
		} else {
			out = reflect.ValueOf(fn).Call(in)
		}

		return out
	})

	return hookCB.Interface()
}

/*
 * after(被hook函数(before(参数)))
 */
func Hook3(fn interface{}, before interface{}, after interface{}) interface{} {
	fnType := reflect.TypeOf(fn)

	hookCB := reflect.MakeFunc(fnType, func(in []reflect.Value) []reflect.Value {

		var out []reflect.Value

		if nil != before {
			if fnType.IsVariadic() {
				out = reflect.ValueOf(before).CallSlice(in)
			} else {
				out = reflect.ValueOf(before).Call(in)
			}
		}

		if fnType.IsVariadic() {
			out = reflect.ValueOf(fn).CallSlice(out)
		} else {
			out = reflect.ValueOf(fn).Call(out)
		}

		if nil != after {
			if fnType.IsVariadic() {
				out = reflect.ValueOf(after).CallSlice(out)
			} else {
				out = reflect.ValueOf(after).Call(out)
			}
		}

		return out
	})

	return hookCB.Interface()
}
