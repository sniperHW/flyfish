## 集群

`table:key`构成唯一的unikey，系统将unikey哈希到一个N以内的值，这个N是固定的，每一个0~N-1的值被称为slot。

即每个unikey会被哈希到一个固定的slot上。

### kvnode的构成

kvnode代表一个进程，有唯一的id。每个kvnode上可以管理n个store（或称为shard）。每个store也具有唯一id。

不同的kvnode上id相同的store构成一个raft group。

slot被分配到确定的store上。

例如，最开始只有唯一的一个store，store:1。它负责0~N-1的所有slot。

随着系统的运行，unikey的数量变多，新增加了一个store:2。现在store:1负责0~N/2-1的slot，store:2负责N/2 ~ N-1的slot。

### slot的分配

pd记录store上负责的slot。

可以只用N bit记录有效slot，如果slot归其管理则设置。

例如store1负责1，5，9 三个slot,则在pd上store1对应的位图1，5，9三个bit就被设置，其它bit为空。

如果N=16384,则每个store需要2k大小用与记录它的slot。10000个store只需要20M内存。

### 安全性保证

任何时候，同1个slot只能被唯一的store管理。在迁移事务中，允许出现暂时性的，某个slot因为没有关联的store而出现无法服务的情况。

### 迁移事务

将slot从store out迁出，迁入到store in。

迁移事务由pd发起，具有唯一单调递增的事务id。事务只能一个一个执行，在前一个事务全部完成之前不允许启动新的事务。

pd生成事务

事务结构

id

Store out:新的slot bitmap

Store in:新的slot bitmap

事务状态

pd向store in和store out发起prepare请求。启动定时器，如果定时器到期未能收到所有的prepare响应，向参与者发送cancel,并将事务标记为失败。

参与者接收到prepare后，将事务持久化，向pd发回响应。

pd接收到所有的prepare响应后，标记事务完成，更新内存中store out和store in的bitmap（后面这三步必须在一次raft proposal中完成）当proposal apply向参与者发送commit。

参与者接收到commit后应用bitmap。

### 故障处理

#### pd

bitmap保存在pd中，为了保证数据安全以及pd的可用性，pd需要部署多个副本，且所有操作都要通过raft完成。

对于事务而言，pd leader首先创建事务结构，发起proposal,当proposal被accept并apply之后才能向参与者发起prepare请求。后续所有需要变更事务状态以及更新bitmap的操作都要走raft。

对于prepare，如果无法形成quorum,prepare请求将无法通过，事务无法开启。

如果发起prepare之后，切换leader,新leader将cancel掉之前的prepare请求。

如果prepare之后无法形成quorum,事务将无法cancel/commit,迁出slot将一直处于锁定状态，无法提供服务。

注：如果给prepare提供deadline即,只要超过deadline,未commit的事务都被认为是cancel的。这样超过deadline之后，迁出store就可以认定事务已经cancel,但是，
此方案依赖精确时间同步。

#### store

store在处理事务的过程中可能会发生故障，发往pd的响应以及pd发过来的commit/cancel可能会丢失。需要保证在出现这两种故障的情况下使得事务得以正确的执行。

store接收到prepare请求后，将事务内容写入到proposal,复制到副本，proposal apply之后发回prepare响应。为了防止因为prepare响应或commit/cancel丢失而卡死在这个状态。store在发回prepare响应之后，设定时器，隔固定间隔重发prepare响应，直到收到pd的commit/cancel。

对于store在发回prepare之后崩溃的情况，raft会选出新的leader，此时leader上也有事务的信息，由leader根据信息重发prepare响应。

store out在接收到prepare请求后，不再处理对迁出kv的请求，对于正在执行的请求，需要等其执行完毕，且kick出内存之后，才将prepare请求复制到副本，prepare请求被apply之后向pd返回响应。在此过程中可能需要kick大量的kv,如果一次性迁出大量slot，kick操作可能花费大量时间而导致事务超时，因此限定事务每次只能迁移一个slot。

store out在响应prepare之后，除非收到cancel,否则不会再处理被迁出的kv。因此，store in只要接收到commit,就可以安全的处理迁入kv。



### kvnode slot数据的初始化

初始时，根据初始store的数量，在pd上预设好slot到store的关系，例如，初始只有1个store，所有slot都归这个store管理。

kvnode初始时store bitmap数据为空，因此登录到pd之后，需要拉取store bitmap。在此之后，slot transfer最终必须得到参与kvnode的确认，即kvnode一定会拥有最新的store bitmap,所以之后再次登录到pd无需再获取store bitmap。



## 定位

client连接proxy,由proxy根据unikey计算所在slot，根据slot得到store leader,将请求路由到store leader所在kvnode。

slot -> store leader kvnode id ->  kvnode对外服务地址

proxy是无状态服务，上述信息需要从pd获取。





## PD职责

协调完成slot的迁移。

监控kvnode运行状况，将负载高的kvnode上部分store leader迁移到负载低的kvnode上。

供kvproxy获取与查询slot定位信息。

pd的失效将导致slot迁移无法完成

如果store leader与pd leader出现连接问题，已经连接上store leader的kvproxy不应受到影响。











































