package conf

var RedisProcessPoolSize    = int(5)
var SqlLoadPoolSize         = int(10)
var SqlUpdatePoolSize       = int(10)
var RedisPipelineSize       = int(50)
var SqlLoadPipeLineSize     = int(50)
var SqlUpdatePipeLineSize   = int(50)
var SqlEventQueueSize       = int(2000)
var RedisEventQueueSize     = int(2000)
var WriteBackEventQueueSize = int(10000)
var MainEventQueueSize      = int(10000)
var MaxPacketSize           = uint64(1024*1024*4)
var WriteBackDelay          = int64(5)