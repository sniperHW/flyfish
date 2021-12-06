### 基本概念

#### flykv

kv存储服务，store的容器。所有kv相关操作将发往flykv处理。

#### flypd

1. 提供配置以及路由信息服务：集群配置由flypd管理和存储，配置的变更，如增/删set,增/删flykv等操作均由flypd处理。flygate从flypd订阅配置信息以更新本地的路由表。flykv启动之后向flypd获取启动信息加载正确的store。
2. slot的负载均衡：将slot均衡到所有的store。
3. flygate目录服务：flygate向flypd上报信息，由flypd维护flygate列表。客户端向pd获取flygate列表，选择一个合适的flygate进行连接。

#### flygate

flykv的网关服务，将来自客户端的请求转发到合适的flykv。

#### slot

key的固定哈希值。计算方式为

`hash(key)%slot_size`

其中slot_size为一个编译固定值。

#### store

实际存储kv的地方。每个store负责一定数量的slot，相关的kv由这个store存储。store可以拥有多个副本，每个副本在不同的flykv上。

#### set

一个flykv节点组。需配置最小节点数量（store的副本数量）以及store数量。set中所有flykv加载的store。set是flyfish系统扩/缩容的最小单位。

