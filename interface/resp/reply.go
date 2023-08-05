package resp

// Reply is the interface of redis serialization protocol message
// tcp需要用字节
type Reply interface {
	ToBytes() []byte
}
