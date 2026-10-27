package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/record"
)

func main() {
	if err := run(); err != nil {
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, "recorder:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("recorder", flag.ContinueOnError)
	wal := fs.String("wal-dir", "", "source member WAL directory (read only)")
	out := fs.String("out", "", "new capture directory (must not exist)")
	snapshot := fs.String("snapshot", "", "existing immutable baseline DB")
	command := fs.String("snapshot-command", "", "JSON argv array for a provider writing CAPTURE_SNAPSHOT_PATH")
	poll := fs.Duration("poll-interval", 100*time.Millisecond, "WAL acquisition interval")
	duration := fs.Duration("duration", 0, "recording duration; zero waits for SIGINT/SIGTERM")
	version := fs.String("etcd-version", "", "required source version: stable 3.4.x, 3.5.x, or 3.6.x")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *wal == "" || *out == "" || *version == "" {
		return fmt.Errorf("usage: recorder --wal-dir DIR --out NEW_DIR --etcd-version VERSION --snapshot FILE [--duration 1h]")
	}
	if *poll <= 0 || *duration < 0 {
		return fmt.Errorf("poll interval must be positive and duration nonnegative")
	}
	var argv []string
	if *command != "" {
		if err := json.Unmarshal([]byte(*command), &argv); err != nil {
			return fmt.Errorf("snapshot-command must be a JSON argv array: %w", err)
		}
		if len(argv) == 0 || argv[0] == "" {
			return fmt.Errorf("snapshot-command cannot be empty")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	err := record.Run(ctx, record.Config{WALDir: *wal, OutputDir: *out, SnapshotSource: *snapshot, SnapshotCommand: argv, PollInterval: *poll, EtcdVersion: *version, Ready: func(m capture.Manifest) {
		fmt.Fprintf(os.Stderr, "recording ready: baseline index=%d revision=%d\n", m.Snapshot.Index, m.Snapshot.Revision)
	}})
	if err != nil {
		return err
	}
	m, err := capture.ReadManifest(*out)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(m)
}
