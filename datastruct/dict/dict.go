package dict

// Consumer is used to traversal dict, if it returns false the traversal will be break
type Consumer func(key string, val interface{}) bool

// Dict is interface of a key-value data structure
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer) //传入方法，遍历
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
