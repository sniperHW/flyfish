# 接入网关

从pd获取配置信息，将slot定位到正确的store（setid+storeid唯一确定），将客户端请求发往store leader，接收到leader响应后将响应返回给客户端。


## 客户端请求的暂存

以下情况需要暂存客户端请求：

* slot找不到store：如果slot正在迁移，在slot成功迁入前，slot属于无主状态。此时flygate需要缓存客户端请求，直到请求超时或有store认领slot。
* store没有leader：此时store需要缓存客户端请求，直到请求超时或store获取leader。在没有leader期间，store需要不断向kvnode查询leader。
* 与leader的连接尚未建立：此时leader需要缓存客户端请求，直到请求超时或连接成功建立。

## 异常事件

#### 与kvnode的连接断开

连接可能因为，空闲，网络异常，或kvnode崩溃导致断开。

考虑因为kvnode崩溃导致的连接断开的情况。在一个多副本的配置中，如果一个store.leader崩溃，那么leader将会转移到另外一个kvnode上。

因此，为了统一处理：与kvnode的连接断开后，要遍历所有的store,如果发现kvnode是store的leader,需要将store.leader置空，触发store重新查询leader的流程。



























