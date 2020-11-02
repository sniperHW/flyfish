# flyfish

为游戏服务器设计的多副本强一致sql缓存，sql支持mysql和pgsql


## 系统结构

	               sqlDB
	                 |
	 -------------------------------------
	 |                                   |
	 |	      raft group             |
	 |                                   |----kvpd 
	 |  flyfishd(leader) flyfish flyfish |     |        
	 -------------------------------------     |         
	                 |                         |        
	              kvproxy-----------------------
	                 |
	               client



## 数据存储

数据在sql库中用传统的表格存储。表格包括`__key__`,`__version__`两个默认字段以及用户自定义的字段。用户自定义字段仅支持int64,uint64,string,float64,blob五种类型。
当记录被访问时，如果记录在本地不存在，被访问的记录将会被载入本地cache。数据库记录到本地cache记录的映射规则如下:

	用table:key合成唯一的键,用hash结构存储,表格中的字段作为hash内容。


假如sql中存在一个user表格，有以下记录

	__key__        age        phone
	lili           10         123456
	bily           12         434443
	chuck          7          34343

在本地被映射为

	key=user:lili field:age = 10 field:phone = 123456
	key=user:bily field:age = 12 field:phone = 434443
	key=user:chuck filed:age = 7 field:phone = 34343


## 表格配置

表格元信息存储在table_conf表中，每一行代表一张表格信息，flyfish启动时会从数据库中读取元信息。

表格配置规则如下：

	字段1:类型:默认值,字段2:类型:默认值,字段3:类型:默认值


例如有如下三张表，table_conf内容如下：

__table__    __conf__              	

	users1      age:int:0,phone:string:123,name:string:haha
	counter     c:int:0
	blob        data:blob:0

对于blob类型会使用0长二进制初始化，所以这里填的默认值0只是占位符，没有实际作用。

## 命令支持

	//按需获取单条记录的字段	
	Get(table,key string,fields ...string)
	
	//获取单条记录的所有字段	
	GetAll(table,key string) 
	
	//设置单条记录的字段，如果提供了version字段会进行版本号校验，只有版本号一致才允许设置	
	Set(table,key string,fields map[string]interface{},version ...int64) 
	
	//设置单条记录，只有当记录不存在时才设置成功	
	SetNx(table,key string,fields map[string]interface{})
	
	//当记录的field内容==oldV时，将其设置为newV,不管设置与否返回field的最新值(除非记录不存在)	
	CompareAndSet(table,key,field string, oldV ,newV interface{}) 
	
	//如果记录不存在用默认值创建记录并将field设为newV,否则同CompareAndSet	
	CompareAndSetNx(table,key,field string, oldV ,newV interface{}) 
	
	//删除一条记录，如果提供了版本号需要执行版本号校验	
	Del(table,key string,version ...int64) 
	
	//将记录的field字段原子增加value,如果记录不存在用默认值创建记录后执行	
	IncrBy(table,key,field string,value int64)  
	
	//将记录的field字段原子减少value,如果记录不存在用默认值创建记录后执行
	DecrBy(table,key,field string,value int64)  
	
	//获取同一table多条记录的指定字段，如果fields没有传将获取所有字段。
	MGet(table string,keys []string,fields ...string) 
	
	//将table中key记录从缓存中踢除，flyfish保证记录回写到数据库之后才踢除
	Kick(table, key string)


## 示例

	package main
	
	import (
		"encoding/binary"
		"fmt"
		kclient "github.com/sniperHW/flyfish/client"
		"github.com/sniperHW/flyfish/errcode"
		"github.com/sniperHW/kendynet/golog"
		"os"
	)
	
	func main() {
	
		kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))
	
		services := []string{}
	
		for i := 1; i < len(os.Args); i++ {
			services = append(services, os.Args[i])
		}
	
		c := kclient.OpenClient(services)
	
		buff := make([]byte, 4)
	
		binary.BigEndian.PutUint32(buff, 100)
	
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["blob"] = buff
		fields["name"] = "sniperHW"
	
		r2 := c.Set("users1", "sniperHW", fields).Exec()
		if r2.ErrCode != errcode.ERR_OK {
			fmt.Println("Set error:", errcode.GetErrorStr(r2.ErrCode))
			return
		}
	
		r3 := c.Get("users1", "sniperHW", "name", "phone", "age", "blob").Exec()
	
		fmt.Println(r3.Fields["name"].GetString())
		fmt.Println(r3.Fields["phone"].GetString())
		fmt.Println(r3.Fields["age"].GetInt())
		fmt.Println(binary.BigEndian.Uint32(r3.Fields["blob"].GetBlob()))
		fmt.Println(r3.Version)
	
	}

## 编译

编译aio版本，此版本只能在linux,mac,freebsd下运行

	go build -tags=aio xxx

编译bio版本，此版本可在任何操作系统下运行

	go build xxx








