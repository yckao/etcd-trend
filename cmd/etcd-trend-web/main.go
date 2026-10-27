package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/explore"
	"github.com/yckao/etcd-trend/internal/webui"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "web:", err)
		os.Exit(1)
	}
}
func run() error {
	fs := flag.NewFlagSet("etcd-trend-web", flag.ContinueOnError)
	runDir := fs.String("run", "", "completed analyzer output directory")
	listen := fs.String("listen", "127.0.0.1:0", "loopback listen address (port 0 chooses an available port)")
	cache := fs.String("cache", "", "new derived index directory; default is temporary and removed on exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *runDir == "" || fs.NArg() != 0 {
		return fmt.Errorf("usage: etcd-trend-web --run DIR [--listen 127.0.0.1:8080] [--cache NEW_DIR]")
	}
	if err := webui.LoopbackAddress(*listen); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintln(os.Stderr, "Validating analysis and building the local query index…")
	store, err := explore.Open(ctx, *runDir, *cache)
	if err != nil {
		return err
	}
	defer store.Close()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: webui.Handler(store), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("Offline explorer: http://%s\n", listener.Addr().String())
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
