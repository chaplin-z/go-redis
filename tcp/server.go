package tcp

/**
 * A tcp handler
 */

import (
	"context"
	"fmt"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config stores tcp handler properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})                                                       // 创建一个用于通知关闭的通道
	sigCh := make(chan os.Signal)                                                          // 创建一个用于接收操作系统信号的通道
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT) //监听信号
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{} // 收到信号后通知关闭服务器
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address) // 在指定地址上监听 TCP 连接
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan) // 调用 ListenAndServe 函数启动服务器，同时传入关闭通道 closeChan
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	go func() {
		<-closeChan //没有值会阻塞在这
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	// listen port
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept() // 监听客户端连接
		if err != nil {
			break
		}
		// handle
		logger.Info("accept link")
		waitDone.Add(1)
		//每有一个客户端连接，就创建一个协程，负责处理这个客户端
		go func() {
			defer func() {
				waitDone.Done() //处理完一个客户端就减一
			}()
			handler.Handle(ctx, conn) //交给resp协议的Handle
		}()
	}
	waitDone.Wait() //阻塞，所有服务完成才退出
}
