// 缓存代理进程。监听 :8080（PROXY_ADDR），回源地址由 ORIGIN_URL 指定。
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cacheproxy/internal/proxy"
)

func main() {
	addr := os.Getenv("PROXY_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	originURL := os.Getenv("ORIGIN_URL")
	if originURL == "" {
		originURL = "http://127.0.0.1:8081"
	}

	// Compose 中两个容器同时启动：短暂等待源站端口就绪。
	waitForOrigin(originURL)

	p, err := proxy.New(originURL)
	if err != nil {
		log.Fatalf("bad ORIGIN_URL %q: %v", originURL, err)
	}

	srv := &http.Server{Addr: addr, Handler: p, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("proxy listening on %s, origin=%s", addr, originURL)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// waitForOrigin 最多等待约 10 秒，让源站端口先起来。
func waitForOrigin(raw string) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return
	}
	host := u.Host
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", host, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("warning: origin %s not reachable yet, starting anyway", host)
}
