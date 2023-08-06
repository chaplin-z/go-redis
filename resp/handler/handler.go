package handler

/*
 * A tcp.RespHandler implements redis protocol
 */

import (
	"context"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// RespHandler implements tcp.Handler and serves as a redis handler
type RespHandler struct {
	activeConn sync.Map // 协议层保存的所有连接
	db         databaseface.Database
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a RespHandler instance
func MakeHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase() //集群db
	} else {
		db = database.NewStandaloneDatabase() //单机版db
	}

	return &RespHandler{
		db: db,
	}
}

// 关闭一个客户端
func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	ch := parser.ParseStream(conn) //把连接交给parse，会不断的读，解析，从管道传出来
	for payload := range ch {
		if payload.Err != nil { //有错误
			//客户端要关闭。如果是四次挥手
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err协议错误
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue //回写成功跳出继续循环
		}
		//如果发来指令是空的
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args) //传到redis内核，db
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler，关闭协议层，所有的
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
