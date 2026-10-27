package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/yckao/etcd-trend/internal/explore"
	"github.com/yckao/etcd-trend/internal/tui"
	"golang.org/x/term"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, in, out *os.File, stderr io.Writer) error {
	fs := flag.NewFlagSet("etcd-trend-tui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runDir := fs.String("run", "", "completed replay output directory")
	cache := fs.String("cache", "", "new derived-index directory; default uses a temporary directory")
	snapshot := fs.Bool("snapshot", false, "print a plain-text snapshot without entering terminal mode")
	from := fs.Int("from", 0, "first analysis window (inclusive, zero based)")
	to := fs.Int("to", -1, "last analysis-window boundary (exclusive); -1 selects through the end")
	metric := fs.String("metric", "writes", "writes, history, retained, compacted, db-growth, free-growth, free-net")
	scope := fs.String("scope", "resource", "resource, object, namespace, api-group, kind, label, namespace-label, controller, manager")
	namespace := fs.String("namespace", "", "filter interval contributions by namespace")
	labelKey := fs.String("label-key", "", "recorded label key to group/filter (defaults to first recorded key)")
	labelValue := fs.String("label-value", "", "exact object label value filter; explicitly empty matches an empty value")
	metadata := fs.Bool("metadata", false, "show metadata instead of the object-detail metric timeline")
	search := fs.String("search", "", "open the object catalog with this search text")
	object := fs.String("object", "", "open this opaque object ID directly")
	width := fs.Int("width", 120, "plain snapshot columns")
	height := fs.Int("height", 36, "plain snapshot rows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *runDir == "" {
		return fmt.Errorf("usage: etcd-trend-tui --run DIR [--snapshot] [--from N --to N] [--metric METRIC]")
	}
	if *width < 20 || *width > 500 || *height < 5 || *height > 200 {
		return fmt.Errorf("snapshot dimensions must be width20-500 and height5-200")
	}
	validMetric := false
	for _, name := range tui.Metrics {
		if name == *metric {
			validMetric = true
		}
	}
	if !validMetric || !explore.ValidScope(*scope) {
		return fmt.Errorf("invalid metric or scope")
	}
	if *from < 0 || *to < -1 || *to >= 0 && *from >= *to {
		return fmt.Errorf("range must satisfy 0 <= from < to, or to=-1 for the end")
	}
	if !*snapshot {
		if !term.IsTerminal(int(in.Fd())) || !term.IsTerminal(int(out.Fd())) {
			return fmt.Errorf("interactive TUI requires a terminal; use --snapshot for plain output")
		}
		if _, err := fmt.Fprintln(stderr, "Preparing offline analysis index..."); err != nil {
			return err
		}
	}
	store, err := explore.Open(ctx, *runDir, *cache)
	if err != nil {
		return err
	}
	defer store.Close()
	labelFilter := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "label-value" {
			labelFilter = true
		}
	})
	cfg := tui.Config{Namespace: *namespace, LabelKey: *labelKey, LabelValue: *labelValue, LabelFilter: labelFilter, Metadata: *metadata, From: *from, To: *to, Metric: *metric, Scope: *scope, Search: *search, ObjectID: *object, Width: *width, Height: *height}
	if *snapshot {
		return tui.Snapshot(ctx, store, cfg, out)
	}
	return tui.Run(ctx, store, cfg, in, out)
}
