## 在基于内存的cache中，全量数据快照并不合理

flyfish是这样一个系统，它cache持久数据源中的热数据，当cache的数据量达到上限时，要加载新的数据必须将别的数据从内存中剔除，
留出空间供新数据使用。

flyfish本身没有持久化cache数据的能力，数据的持久化完全是由raft log以及snapshot提供的。

flyfish的持久化模型如下：

初始时cache为空

flyfish接收客户端操作，从数据源加载并操作数据。每一条操作被记录为一个raft log.

当flyfish重启之后，重播raft log就完成了内存的恢复。

raft snapshot可以看作raft log压缩的手段。

当raft log累积到一定长度之后，flyfish对内存数据做一个快照，存放到raft snapshot中，这样之前的raft log就可以丢弃。之后的raft log记录的操作都是以
raft snapshot中的内存快照作为初始点。


考虑到在两次快照之间，并不是所有数据都发生了变更，因此，只对发生了变更的数据做快照是一个更合理的设计。

如果flyfish从空开始。

第一次快照

内存中变更数据集合为S0(之前内存是空的，此时内存中所有数据都是变更数据，因此S0包含内内存中所有的数据)

第二次快照

变更数据集合为S1，S1中可能包含S0中存在的数据，也可能包含S0中不存在的数据。

那么当flyfish重启的时候，要恢复的数据是

S0与S1的并集。

随着快照数量的增加，需要将S0,S1.....Sn做一次合并，压缩快照。

## 实现

### snapshot

每个kv设置一个snapshot标记

kvmgr设置一个kicks字典

每当一个kv被替换，将key加入kicks中。

当kv被成功加载或变更设置其snapshot标记。

执行snapshot的时候将标记为snapshot的kv先写入snapshot，之后将kicks中的key写入snapshot。

完成后清空kicks，清除kv的snapshot标记。

### snapshot merge

加载所有的snapshot，根据term,index按升序排序。

构造一个空的kvstore。

按序执行snapshot文件中的指令。

指令执行完毕后将恢复出一个kvstore。

保存这个kvstore，用最后一个snapshot文件的term和index产生一个snapshot文件。这个文件将会覆盖最近的一个snapshot文件。

清除其它snapshot文件。

















































































