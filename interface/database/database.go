package database

import (
	"go-redis/interface/resp"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// Database is the interface for redis style storage engine
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply //执行
	AfterClientClose(c resp.Connection)
	Close()
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct { //指代redis的数据结构
	Data interface{}
}
