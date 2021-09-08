静态hash版本


部署固定数量的kvstore,将unikey哈希到slot。在将slot哈希到kvstore。

静态hash版本无法在运行中扩容/缩容。


如果向扩/缩容，需要停止所有的kvnode。清除缓存。从配置中增/删kvnode,启动系统。






