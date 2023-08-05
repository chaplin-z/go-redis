package consistenthash

import (
	"hash/crc32"
	"sort"
)

// HashFunc defines function to generate hash code
type HashFunc func(data []byte) uint32

// NodeMap stores nodes and you can pick node from NodeMap
// 哈希节点
type NodeMap struct {
	hashFunc    HashFunc       //哈希函数
	nodeHashs   []int          // sorted，节点的哈希值，各个哈希范围
	nodehashMap map[int]string //哈希值：地址
}

// NewNodeMap creates a new NodeMap
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in NodeMap
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// AddNode add the given nodes into consistent hash circle
// 加节点：加哈希值，并排序
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	sort.Ints(m.nodeHashs)
}

// PickNode gets the closest item in the hash to the provided key.
// 判断需要进入几号节点
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hashFunc([]byte(key)))

	// Binary search for appropriate replica.搜索节点的序号
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})

	// Means we have cycled back to the first replica.
	if idx == len(m.nodeHashs) {
		idx = 0
	}

	return m.nodehashMap[m.nodeHashs[idx]]
}
