


##kvnode构成

每个kvnode节点通过1个16位整形标识。

kvnode上运行多个kvstore,每个kvstore代表一个region。

每个region由一个16位整形标。

每个kvstore由一个id标识，其高16位位节点id,低16位为region id。

region id相同的kvstore构成raftgroup。

##kvproxy

client连接kvnode的代理通信节点。其职责如下：

* 由少量kvproxy与kvnode建立连接，大量client与kvproxy建立连接，减少kvnode管理的连接数量

* 根据请求的key,把请求转发给正确region的master节点。

##kvpd












