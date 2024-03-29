# 长连接服务的负载均衡

请求转发代理，连接数据服务后端，将客户端请求路由到数据后端。客户端到代理基于TCP长连接，在无故障的情况下持续保持连接。客户端请求采用len+data的包结构。

因为客户端到代理采用了长连接，所有请求都使用同一个连接发送，因此无法采用短连接针对每个请求动态选择代理的负载均衡方案。

要做负载均衡，首先要定义任务量。代理服务的工作是接收客户端的请求，保留请求上下文，将请求路由到数据后端，当数据后端返回应答后，根据保存的请求上下文，将应答返回给客户端。因此，可以将请求数/秒定义为代理的工作量。负载均衡的目的就是将来自多个客户端的请求均衡的分摊到数据代理上。

### 算法

代理记录接收到的请求数/秒的移动平均数，定期向目录服务上报。

客户端记录发送的请求数/秒的移动平均数，定期向目录服务拉取代理列表。

客户端拉取到列表之后，计算出单个代理的均衡请求数/秒。如果发现自己当前连接的代理超过均衡请求数，则向目录服务请求切换代理，请求中携带客户端当前连接的代理，以及客户端的请求数/秒的移动平均数。

目录服务接收到客户端的代理切换请求，根据本地缓存的代理列表计算单个代理的均衡请求数/秒，选择一个负载量最低的代理。

代理切换的条件

1）原代理的负载量-客户端发送量 > 均衡值

2）负载最低的代理负载量+客户端发送量 < 均衡值乘以一个固定系数

如果满足以上两个条件，更新

原代理的负载量 =- 客户端发送量

负载最低的代理负载量 =+ 客户端发送量

并将负载最低的代理作为应答返回给客户端。









































































