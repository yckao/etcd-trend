package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/yckao/etcd-trend/internal/inspect"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "inspect:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("etcd-trend-inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runDir := fs.String("run", "", "completed analyzer output directory")
	window := fs.String("window", "all", "zero-based window number, or all")
	metric := fs.String("metric", "writes", "writes, history, retained, compacted, db-growth, free-growth, or free-net")
	scope := fs.String("scope", "both", "resource, object, or both")
	limit := fs.Int("limit", 10, "maximum rows per scope (1-100; limited by the analyzer's stored --top)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *runDir == "" {
		return fmt.Errorf("usage: etcd-trend-inspect --run DIR [--window N|all] [--metric writes|history|retained|compacted|db-growth|free-growth|free-net]")
	}
	selected := int64(-1)
	if *window != "all" {
		var err error
		selected, err = strconv.ParseInt(*window, 10, 64)
		if err != nil || selected < 0 {
			return fmt.Errorf("window must be all or a nonnegative zero-based index")
		}
	}
	return inspect.Run(ctx, inspect.Config{RunDir: *runDir, Window: selected, Metric: *metric, Scope: *scope, Limit: *limit}, stdout)
}
