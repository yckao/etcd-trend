package loadtestcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yckao/etcd-trend/internal/cli/stdio"
	"github.com/yckao/etcd-trend/internal/loadtest"
)

func Run(ctx context.Context, args []string, streams stdio.Streams) error {
	streams = streams.WithDefaults()
	fs := stdio.Flags("loadtest", streams.Err)
	dir := fs.String("out", "", "new qualification directory")
	duration := fs.Duration("duration", time.Minute, "paced synthetic workload duration")
	rate := fs.Int64("bytes-per-second", 2_000_000, "target value bytes per second (WAL framing adds overhead)")
	value := fs.Int("value-bytes", 32*1024, "value bytes per update")
	keys := fs.Int("keys", 128, "rolling key count")
	recorder := fs.String("recorder", "", "optional external legacy recorder executable; default runs this executable's record command")
	analyzer := fs.String("analyzer", "", "optional external legacy analyzer executable; default runs this executable's replay command")
	workload := fs.String("workload", "raw", "synthetic workload: raw or kubernetes")
	recordOnly := fs.Bool("record-only", false, "finish after capture and source digest; replay separately")
	compact := fs.Duration("compaction-interval", 0, "physical compaction interval; default raw 5s, kubernetes 45s")
	if err := fs.Parse(args); err != nil {
		return stdio.UsageError{Err: err}
	}
	if *dir == "" || fs.NArg() != 0 {
		return stdio.UsageError{Err: fmt.Errorf("usage: etcd-trend loadtest --out NEW_DIR [--duration 1m]")}
	}
	for _, path := range []*string{recorder, analyzer} {
		if *path != "" {
			absolute, err := filepath.Abs(*path)
			if err != nil {
				return err
			}
			*path = absolute
		}
	}
	summary, err := loadtest.Run(ctx, loadtest.Config{Dir: *dir, Recorder: *recorder, Analyzer: *analyzer, Duration: *duration, BytesPerSecond: *rate, ValueBytes: *value, Keys: *keys, Workload: *workload, RecordOnly: *recordOnly, CompactionInterval: *compact})
	if err != nil {
		return err
	}
	return json.NewEncoder(streams.Out).Encode(summary)
}
