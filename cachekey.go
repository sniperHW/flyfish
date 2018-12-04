package flyfish

import (
	//"github.com/sniperHW/kendynet/util"
	//"time"
	//"unsafe"
	//"fmt"
)

const (
	cache_new     = 1
	cache_ok      = 2
	cache_missing = 3
)

type cacheKey struct {
	uniKey     string
	idx        uint32
	lastAccess uint64 //time.Time
	version    int64
	status     int    			
	locked     bool    			//操作是否被锁定	
	cmdQueue   keyCmdQueue
	meta      *table_meta
}

var cacheKeys map[string]*cacheKey
//var minHeap *util.MinHeap
var tick uint64  //每次访问+1假设每秒访问100万次，需要运行584942年才会回绕


/*func (this *cacheKey) Less(o util.HeapElement) bool {
	return this.lastAccess < o.(*cacheKey).lastAccess
	//return this.lastAccess.Before(o_.lastAccess)
}

func (this *cacheKey) GetIndex() uint32 {
	return this.idx
}
	
func (this *cacheKey) SetIndex(idx uint32) {
	this.idx = idx
}*/

func (this *cacheKey) setMissing() {
	Debugln("SetMissing key:",this.uniKey)
	this.version = 0
	this.status  = cache_missing 
}

func (this *cacheKey) setOK(version int64) {
	this.version = version
	this.status  = cache_ok
}

func (this *cacheKey) reset() {
	this.status = cache_new
}


/*
UpdateLRU和newCacheKey只能再主消息循环中访问，所以tick不需要加锁保护
*/

func (this *cacheKey) updateLRU() {
	tick++
	//this.lastAccess = tick//time.Now()
	//minHeap.Insert(this)	
}

func newCacheKey(table string,uniKey string) *cacheKey {
	
	meta := GetMetaByTable(table)

	if nil == meta {
		Errorln("newCacheKey key:",uniKey," error,[missing table_meta]")
		return nil
	}

	tick++

	k := &cacheKey {
		uniKey     : uniKey,
		lastAccess : tick,
		status     : cache_new,
		meta       : meta,
	}

	Debugln("newCacheKey key:",uniKey)

	cacheKeys[uniKey] = k
	//minHeap.Insert(k)
	return k
}

func getCacheKey(table string,uniKey string) *cacheKey {
	k,ok := cacheKeys[uniKey]
	if ok {
		k.updateLRU()
		return k
	} else {
		return newCacheKey(table,uniKey)
	}
}

/*
func lruMinKey() *cacheKey {
	min := minHeap.Min()
	if nil == min {
		return nil
	} else {
		return min.(*cacheKey)
	}
}

func removeCacheKey(key *cacheKey) {
	delete(cacheKeys,key.uniKey)
	minHeap.Remove(key)
}
*/
func clearCacheKey() {
	cacheKeys = map[string]*cacheKey{}
	//minHeap   = util.NewMinHeap(1000000)	
}


func init() {
	clearCacheKey()
}