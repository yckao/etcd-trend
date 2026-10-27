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

	"github.com/yckao/etcd-trend/internal/loadtest"
)

func main() {
	dir := flag.String("out", "", "new qualification directory")
	duration := flag.Duration("duration", time.Minute, "paced synthetic workload duration")
	rate := flag.Int64("bytes-per-second", 2_000_000, "target value bytes per second (WAL framing adds overhead)")
	value := flag.Int("value-bytes", 32*1024, "value bytes per update")
	keys := flag.Int("keys", 128, "rolling key count")
	recorder := flag.String("recorder", "bin/recorder", "recorder binary")
	analyzer := flag.String("analyzer", "bin/analyzer", "analyzer binary")
	workload := flag.String("workload", "raw", "synthetic workload: raw or kubernetes")
	recordOnly := flag.Bool("record-only", false, "finish after capture and source digest; replay separately")
	compact := flag.Duration("compaction-interval", 0, "physical compaction interval; default raw 5s, kubernetes 45s")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: etcd-trend-loadtest --out NEW_DIR [--duration 1m]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := filepath.Abs(*recorder)
	if err == nil {
		var a string
		a, err = filepath.Abs(*analyzer)
		if err == nil {
			var summary *loadtest.Summary
			summary, err = loadtest.Run(ctx, loadtest.Config{Dir: *dir, Recorder: r, Analyzer: a, Duration: *duration, BytesPerSecond: *rate, ValueBytes: *value, Keys: *keys, Workload: *workload, RecordOnly: *recordOnly, CompactionInterval: *compact})
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(summary)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}
