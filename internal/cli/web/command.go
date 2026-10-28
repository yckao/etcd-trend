package webcmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/yckao/etcd-trend/internal/cli/stdio"
	"github.com/yckao/etcd-trend/internal/explore"
	"github.com/yckao/etcd-trend/internal/webui"
)

func Run(ctx context.Context, args []string, streams stdio.Streams) error {
	streams = streams.WithDefaults()
	stdout, stderr := streams.Out, streams.Err
	fs := stdio.Flags("web", stderr)
	runDir := fs.String("run", "", "completed analyzer output directory")
	listen := fs.String("listen", "127.0.0.1:0", "loopback listen address (port 0 chooses an available port)")
	cache := fs.String("cache", "", "new derived index directory; default is temporary and removed on exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" || fs.NArg() != 0 {
		return fmt.Errorf("usage: etcd-trend web --run DIR [--listen 127.0.0.1:8080] [--cache NEW_DIR]")
	}
	if err := webui.LoopbackAddress(*listen); err != nil {
		return err
	}
	fmt.Fprintln(stderr, "Validating analysis and building the local query index…")
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
	fmt.Fprintf(stdout, "Offline explorer: http://%s\n", listener.Addr().String())
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
