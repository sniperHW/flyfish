## kvnode加载store

kvnode启动后使用id以及hostIP向flypd查询store信息。加载由flypd返回的store。

## 初始静态配置

构建slot与store的关系表，以及kvnode与store的关系表。

此配置初始时构建，构建完成后存储在flypd。

## 动态配置变更

在运行过程中增加/移除kvnode或store。

为简单起见，扩容和缩容都以set为单位。每次添加/删除一个组。

我们限定一个raft group所必须的最少副本数量（只能取1,3,5....）。

则每个set必须包含最少副本数量的机器。

我们还可以限定每个set包含固定数量的store，例如5个。

#### 给store添加/移除kvnode

如果store的某个副本因为物理机器出现长时间无法恢复的故障，为了保障数据的可靠性，需要添加新的副本，以及将故障副本移除。

此时需要向store添加一个新的kvnode，以及将故障kvnode移除。

因为我们已经以set为单位，一个store的副本发生故障，其实跟它在同一个set的store副本都发生故障。因此我们可以直接向set添加新的kvnode以及移除故障kvnode。

##### 添加kvnode

向pd发出请求，pd检查上传的set是否正确，nodeid以及地址是否否重复。

检查通过后持久化添加请求。并生成一个pendingAdd结构。然后向set中所有节点发出添加请求。因为每个节点上有相同的store。

节点接到请求后，向每个store所在raft group发起添加成员请求，只有是leader的store才执行请求。当请求apply之后向pd发回响应。

只有当所有store都发回添加响应，添加事务才算完成。此时可以将新的kvnode加入set的配置中。

kvnode被加入配置之后新的kvnode就可以启动。



##### 移除kvnode

与添加类似，生成一个pendingRem结构，向set中节点发出remkvnode请求，接收到所有store的响应后再将kvnode从配置移除。



#### 添加新的store，并为其关联kvnode

当系统扩容时，需要添加新的store，并为新的store关联kvnode。当新加入的store投入运行之后。flypd就可以将部分slot迁移到新的store中。

假如当前已经存在两个set（1，2）。

则组1的store编号分别为(0,1,2,3,4)

组2的store编号分别为(5,6,7,8,9)

现在我们要添加新的set3（假设最少副本数量为3）。

则我们需要编辑一个列表

`Set 3 {`

 `Kvnode{`

   `id:31`

   `host:"x.x.x.x"`

`}`

 `Kvnode{`

   `id:32`

   `host:"x.x.x.x"`

`}`

 `Kvnode{`

   `id:33`

   `host:"x.x.x.x"`

`}`

`}`

将这个列表提交给flypd，flypd将生成一份配置。将store(10,11,12,13,14)分配给kvnode:31-33。

之后就可以启动新的kvnode去加载对应的store。



#### 移除store

缩容同样以set为单位。

例如现在要将set 3移除。

向flypd请求移除set 3。flypd将把store(10,11,12,13,14)标记为待移除。然后执行流程开始把store(10,11,12,13,14)上面的slot迁移出去。

当所有slot迁移完成，store(10,11,12,13,14)将不再负责任何得slot。此时，将set 3从配置中移除。可以将set 3上的机器关闭。



## slot迁移

当新的set加入集群后，pd需要把现有set中的slot迁移到新加入的set中。pd首先根据当前set的数量，计算每个set的平均slot数量。

把高于平均数量的set中的slot迁移到低于平均数量的set中。

slot在迁移过程中将无法服务，为了避免大量slot同时进入无法服务的状态，应该挨个slot迁移。这样带来的副作用是整个平衡过程需要一定时间。

### 迁移事务

pd创建一个迁移事务(slot,storeTransOut,transOutOk,storeTransIn)，持久化，向storeTransOut leader发出迁出通告。

storeTransOut leader接收到通告后，将slot关联的kv踢出缓存。当所有kv都被踢出之后，向pd返回响应。pd标记transOutOk=true,

迁出部分完成。然后向storeTransIn leader发出slot迁入通告，storeTransIn leader响应，事务结束，pd将迁移事务删除。



## 路由

client连接flygate,将请求发往flygate,flygate根据请求的key计算出slot。根据slot将请求发往正确的kvnode。

### flygate启动

flygate在启动时向flypd获取配置信息并缓存在本地。





































































































