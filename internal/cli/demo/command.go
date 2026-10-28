// Package democmd creates and uses only its own temporary loopback etcd member.
package democmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yckao/etcd-trend/internal/cli/stdio"
	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/internal/qualification"
)

func Run(ctx context.Context, args []string, streams stdio.Streams) error {
	streams = streams.WithDefaults()
	stdout, stderr := streams.Out, streams.Err
	fs := stdio.Flags("demo", stderr)
	out := fs.String("out", "", "new demo directory (default: a temporary directory)")
	updates := fs.Int("updates", 240, "number of hot-object updates")
	size := fs.Int("value-bytes", 300*1024, "size of the repeatedly updated value")
	interval := fs.Duration("interval", 2*time.Millisecond, "delay between fixture updates")
	if err := fs.Parse(args); err != nil {
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
	s, err := qualification.Run(ctx, qualification.Config{Dir: *out, Workload: fixture.Workload{Updates: *updates, ValueBytes: *size, Interval: *interval}})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
