slot迁移

1） slot的迁移分两种情况，新增加set之后，将现有store中的slot迁移到新增set的store中
2） 删除一个set之前，需要将待删除set所有store中的slot迁移到保留set的store中。

slot迁移的过程中归属这个slot的kv将暂时无法服务。为了避免出现大量kv同时暂停服务，应该避免同时迁移大量slot。
但是一个一个slot的迁移可能导致整体的迁移时间太长，所以应该选择一个合适的同时迁移数量。

新增set或有set被标记为clear slot之后启动slot balance处理


slot balance处理

slot balance处理是一个持续的流程，直到认定slot已经分配平衡为止。

slot平衡条件：任一store的slot数量不大于slot.SlotCount/setCount * StorePerSet + 1


slot balance优先处理标记为clear的set。


flykv副本故障


1）副本短时间内无法重启，但数据文件可以拷贝


关键副本故障导致raftgroup无法服务

唯一副本故障：在pd中直接修改故障flykv实例的服务地址，将数据文件拷贝到新的节点，重启flykv实例。

副本故障导致无法形成多数集：


















































