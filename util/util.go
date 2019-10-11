package util

// 字符串转为16位整形值
func StringHash(s string) int {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}

	return int(hash)
}

func Must(i interface{}, e error) interface{} {
	if e != nil {
		panic(e)
	}
	return i
}

func IsPow2(size int) bool {
	return (size & (size - 1)) == 0
}

func SizeOfPow2(size int) int {
	if IsPow2(size) {
		return size
	}
	size = size - 1
	size = size | (size >> 1)
	size = size | (size >> 2)
	size = size | (size >> 4)
	size = size | (size >> 8)
	size = size | (size >> 16)
	return size + 1
}
