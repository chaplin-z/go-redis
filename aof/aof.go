package aof

import (
	"go-redis/config"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

// 写在channel里的数据格式
type payload struct {
	cmdLine CmdLine //用户的指令
	dbIndex int     //要用的db
}

// AofHandler receive msgs from channel and write to AOF file
type AofHandler struct {
	db          databaseface.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
	currentDB  int
}

// NewAOFHandler creates a new aof.AofHandler
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.LoadAof(0) //初始化的时候就把aof文件加载redis里
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	// 开协程，一直从channel中取
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
// payLoad(set k v)->aofChan，往channel里写数据
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
// 取出channel的数据，写在磁盘里,每个客户端连接对应一个aof协程，所以要加锁，防止冲突
func (handler *AofHandler) handleAof() {
	// serialized execution
	handler.currentDB = 0
	for p := range handler.aofChan {
		handler.pausingAof.RLock()          // 防止其他goroutine暂停aof
		if p.dbIndex != handler.currentDB { // 如果切换了db
			// select db
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // skip this command
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		handler.pausingAof.RUnlock()
	}
	handler.aofFinished <- struct{}{}
}

// LoadAof read aof file，重启redis时把aof文件刷到redis里
func (handler *AofHandler) LoadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := &connection.FakeConn{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

// Close gracefully stops aof persistence procedure
func (handler *AofHandler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinished // wait for aof finished
		err := handler.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}
