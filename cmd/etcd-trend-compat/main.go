// etcd-trend-compat starts only an isolated external etcd release fixture.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/compatfixture"
	"github.com/yckao/etcd-trend/internal/fixture"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "compat:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("etcd-trend-compat", flag.ContinueOnError)
	binary := fs.String("etcd-binary", "", "official etcd release binary or pinned source build")
	out := fs.String("out", "", "new compatibility run directory (default: temporary)")
	updates := fs.Int("updates", 96, "number of repeated source updates")
	size := fs.Int("value-bytes", 16*1024, "bytes in each updated value")
	interval := fs.Duration("interval", 2*time.Millisecond, "delay between source updates")
	speeds := fs.String("speeds", "1,100,max", "comma-separated replay pacing rates")
	captureOnly := fs.Bool("capture-only", false, "record source and expected state without replay")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *binary == "" || *updates < 3 || *size < 64 || *interval < 0 {
		return fmt.Errorf("usage: etcd-trend-compat --etcd-binary PATH [--out NEW_DIR]; updates >= 3 and value-bytes >= 64 required")
	}
	var rates []float64
	for _, value := range strings.Split(*speeds, ",") {
		if value == "max" {
			rates = append(rates, 0)
			continue
		}
		n, err := strconv.ParseFloat(value, 64)
		if err != nil || n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
			return fmt.Errorf("invalid replay speed %q", value)
		}
		rates = append(rates, n)
	}
	if *out == "" {
		parent, err := os.MkdirTemp("", "etcd-trend-compat-")
		if err != nil {
			return err
		}
		*out = filepath.Join(parent, "run")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	s, err := compatfixture.Run(ctx, compatfixture.Config{Binary: *binary, Dir: *out, Workload: fixture.Workload{Updates: *updates, ValueBytes: *size, Interval: *interval}, Speeds: rates, CaptureOnly: *captureOnly})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
