package hutils

func Must(i interface{}, e error) interface{} {
	if e != nil {
		panic(e)
	}
	return i
}
