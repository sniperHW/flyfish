[![Build Status](https://travis-ci.org/hqpko/hbuffer.svg?branch=master)](https://travis-ci.org/hqpko/hbuffer)

# Buffer
go Buffer.
使用方便、可复用

```go
package main

import (
	"fmt"

	"github.com/hqpko/hbuffer"
)

func main() {
	by := hbuffer.NewBuffer()

	by.WriteBool(true)
	by.WriteInt32(123)
	by.WriteInt64(124)
	by.WriteUint32(125)
	by.WriteUint64(126)
	by.WriteFloat32(122.33)
	by.WriteFloat64(122.44)
	by.WriteString("test_abc一二三")

	// read from position 0
	by.SetPosition(0)

	boo := by.ReadBool()
	fmt.Println(boo) // true

	i32,_ := by.ReadInt32()
	fmt.Println(i32) // 123

	i64,_ := by.ReadInt64()
	fmt.Println(i64) // 124

	ui32,_ := by.ReadUint32()
	fmt.Println(ui32) // 125

	ui64,_ := by.ReadUint64()
	fmt.Println(ui64) // 126

	f32,_ := by.ReadFloat32()
	fmt.Println(f32) // 122.33

	f64,_ := by.ReadFloat64()
	fmt.Println(f64) // 122.44

	s,_ := by.ReadString()
	fmt.Println(s) // test_abc一二三
}

```
