


##kvnode构成

每个kvnode节点通过1个16位整形标识。

kvnode上运行多个kvstore,每个kvstore代表一个region。

每个region由一个16位整形标。

每个kvstore由一个id标识，其高16位为节点id,低16位为region id。

region id相同的kvstore构成raftgroup。

##kvproxy

client连接kvnode的代理通信节点。其职责如下：

* 由少量kvproxy与kvnode建立连接，大量client与kvproxy建立连接，减少kvnode管理的连接数量

* 根据请求的key,把请求转发给正确region的master节点。

##kvpd

管理key到region的分配.

kvproxy通告key向kvpd请求，kvpd把管理这个key的region所在master节点地址返回给kvproxy
kvproxy向这个master节点请求服务。

key到region的分配信息可以缓存在kvproxy本地。一旦信息与kvnode不一致(例如那个key已经不归不归kvnode上的region管理，或kvnode丢失leadership),
kvnode拒绝请求，kvproxy再次向kvpd获取更新信息。

对于kvnode故障无法再恢复的情况，kvproxy缓存了分配信息，但无法connect上目标kvnode。因此，每次connect失败，kvproxy都应该向kvpd请求刷新分配信息。

key被计算成一个64位整形值N。kvpd负责把N映射到region。

###kvpd作为kvnode集群控制器

* kvnode向kvpd上报自己的region,以及服务地址。
* kvpd根据上报的region找到raftgroup,让新的kvnode加入raftgroup(走raft本身的配置变更流程)。
* kvpd应该尽量让leader均匀分布在不同的节点上，当kvpd发现leader过于集中，选择一个空闲节点，请求原leader通过raft leader转让流程将leader转让到空闲节点上。

###key到region的映射





















