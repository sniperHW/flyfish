# flyfish

为游戏服务器设计的pgsql/redis冷热处理服务。



## 系统结构

	redis      pgsql
	   \      /
	   flyfishd
	      |
            client


## 数据存储

数据在pgsql库中用传统的表格存储。表格包括`__key__`,`__version__`两个默认字段以及用户自定义的字段。用户自定义字段仅支持int64,uint64,string,float64四种类型。
当记录被访问时，如果记录在redis中不存在，被访问的记录将会被写入到redis。数据库记录到redis记录的映射规则如下:

	用table:key合成唯一的redis键,用hash结构存储,表格中的字段作为hash内容。


假如sql中存在一个user表格，有以下记录

	__key__        age        phone
	lili           10         123456
	bily           12         434443
	chuck          7          34343

在redis中被映射为

	key=user:lili field:age = 10 field:phone = 123456
	key=user:bily field:age = 12 field:phone = 434443
	key=user:chuck filed:age = 7 field:phone = 34343


## 命令支持

	Get(table,key string,fields ...string)  //按需获取单条记录的字段

	GetAll(table,key string) //获取单条记录的所有字段

	Set(table,key string,fields map[string]interface{},version ...int64) //设置单条记录的字段，如果提供了version字段会进行版本号校验，只有版本号一致才允许设置

	SetNx(table,key string,fields map[string]interface{}) //设置单条记录，只有当记录不存在时才设置成功

	CompareAndSet(table,key,field string, oldV ,newV interface{}) //当记录的field内容==oldV时，将其设置为newV,不管设置与否返回field的最新值(除非记录不存在)

	CompareAndSetNx(table,key,field string, oldV ,newV interface{}) //如果记录不存在用默认值创建记录并将field设为newV,否则同CompareAndSet

	Del(table,key string,version ...int64) //删除一条记录，如果提供了版本号需要执行版本号校验

	IncrBy(table,key,field string,value int64)  //将记录的field字段原子增加value,如果记录不存在用默认值创建记录后执行

	DecrBy(table,key,field string,value int64)  //将记录的field字段原子减少value,如果记录不存在用默认值创建记录后执行

	MGet(table string,keys []string,fields ...string) //获取同一table多条记录的指定字段，如果fields没有传将获取所有字段。

	Scaner(table string,fileds ...string) //遍历table表，返回记录，如果fields没有传将获取所有字段。(此命令从pgsql直接获取数据，且不会将数据缓存到redis)

## redis缓存

游戏服务器通过代理服务访问数据，对于查询请求，代理首先查看缓存中是否存在，如果不存在则从pgsql数据库加载记录，并将数据写入到redis,之后响应查询请求。

对于更新请求，代理首先更新redis中的数据，成功后响应更新请求，然后将pgsql回写请求投入回写队列，
pgsql回写处理器以批处理的形式执行回写。

## 缓存替换

flyfish通过内存中的LRU缓存来执行redis数据的替换，当key的数量超过设置的缓存数量后，通过LRU把最久
未访问过的key淘汰出redis。

## 示例 获取多条记录


	func MGet(c *kclient.Client) {

		keys := []string{"huangwei:1","huangwei:2","huangwei:3","huangwei:xx"}

		mget := c.MGetAll("users1",keys)

		mget.Exec(func(ret *kclient.MutiResult){
			if ret.ErrCode == errcode.ERR_OK {
				for _,v := range(ret.Rows) {
					if nil == v.Fields {
						fmt.Println(v.Key,"not exist")
					} else {
						fmt.Println(v.Key,"age",v.Fields["age"].GetInt())					
					}
				}
			} else {
				fmt.Println(errcode.GetErrorStr(ret.ErrCode))
			}
		})
	}


	func main() {

		services := []string{"127.0.0.1:10012"}
		c := kclient.OpenClient(services)

		MGet(c)

		sigStop := make(chan bool)
		_, _ = <-sigStop
	}










