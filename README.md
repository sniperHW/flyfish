# flyfish

为游戏服务器设计的sql/redis冷热处理服务。


## 数据表记录到redis的映射

数据以普通表格存储在sql数据库中，通过redis的hash映射数据库表中的每一行记录。

例如：
sql中存在一个user表格有以下记录

	__key__        age        phone
	lili           10         123456
	bily           12         434443
	chuck          7          34343

在redis中被映射为

	key=user:lili field:age = 10 field:phone = 123456
	key=user:bily field:age = 12 field:phone = 434443
	key=user:chuck filed:age = 7 field:phone = 34343

## redis缓存

游戏服务器通过代理服务访问数据，对于查询请求，代理首先查看缓存中是否存在，如果不存在则从
sql数据库加载记录，并将数据写入到redis,之后响应查询请求。

对于更新请求，代理首先更新redis中的数据，成功后响应更新请求，然后将sql回写请求投入回写队列，
sql回写处理器以批处理的形式执行回写。

## 缓存替换

flyfish通过内存中的LRU缓存来执行redis数据的替换，当key的数量超过设置的缓存数量后，通过LRU把最久
未访问过的key淘汰出redis。







