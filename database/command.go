package database

import (
	"strings"
)

var cmdTable = make(map[string]*command) //记录指令
// 指令
type command struct {
	executor ExecFunc //方法，set，get等都要实现这个
	arity    int      // 参数个数 allow number of args, arity < 0 means len(args) >= -arity
}

// RegisterCommand registers a new command
// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
// for example: the arity of `get` is 2, `mget` is -2
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
