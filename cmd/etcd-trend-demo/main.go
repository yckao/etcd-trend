// etcd-trend-demo creates and uses only its own temporary loopback etcd member.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/internal/qualification"
)

func main() {
	if err := run(); err != nil {
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("etcd-trend-demo", flag.ContinueOnError)
	out := fs.String("out", "", "new demo directory (default: a temporary directory)")
	updates := fs.Int("updates", 240, "number of hot-object updates")
	size := fs.Int("value-bytes", 300*1024, "size of the repeatedly updated value")
	interval := fs.Duration("interval", 2*time.Millisecond, "delay between fixture updates")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *updates < 3 || *size < 64 || *interval < 0 {
		return fmt.Errorf("updates >= 3, value-bytes >= 64 and interval >= 0 are required")
	}
	if *out == "" {
		parent, err := os.MkdirTemp("", "etcd-trend-demo-")
		if err != nil {
			return err
		}
		*out = filepath.Join(parent, "run")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := qualification.Run(ctx, qualification.Config{Dir: *out, Workload: fixture.Workload{Updates: *updates, ValueBytes: *size, Interval: *interval}})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
