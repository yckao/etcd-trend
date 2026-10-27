// Package loadtest measures the recorder and analyzer as separate processes
// against a private loopback fixture. It never accepts a production endpoint.
package loadtest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/internal/replay"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Dir, Recorder, Analyzer string
	Duration                time.Duration
	BytesPerSecond          int64
	ValueBytes, Keys        int
	Workload                string
	RecordOnly              bool
	CompactionInterval      time.Duration
}

type Process struct {
	WallNS           int64   `json:"wall_ns"`
	UserCPUSeconds   float64 `json:"user_cpu_seconds"`
	SystemCPUSeconds float64 `json:"system_cpu_seconds"`
	PeakRSSBytes     int64   `json:"peak_rss_bytes"`
}

type Summary struct {
	Workload                string  `json:"workload"`
	RecordOnly              bool    `json:"record_only"`
	Phases                  []Phase `json:"phases,omitempty"`
	SourceVersion           string  `json:"source_version"`
	Platform                string  `json:"platform"`
	TargetBytesPerSecond    int64   `json:"target_value_bytes_per_second"`
	ValueBytes              int     `json:"value_bytes"`
	Keys                    int     `json:"rolling_keys"`
	Updates                 int     `json:"updates"`
	WorkloadNS              int64   `json:"workload_duration_ns"`
	ValueBytesWritten       int64   `json:"value_bytes_written"`
	WALBytes                int64   `json:"wal_bytes"`
	WALSegments             int     `json:"wal_segments"`
	WALBytesPerSecond       float64 `json:"wal_bytes_per_workload_second"`
	IndexBytes              int64   `json:"index_bytes"`
	AnchorBytes             int64   `json:"anchor_index_bytes"`
	PrepareNS               int64   `json:"prepare_duration_ns"`
	ReplayNS                int64   `json:"replay_duration_ns"`
	ReplayWALBytesPerSecond float64 `json:"replay_wal_bytes_per_second"`
	ExpectedRevision        int64   `json:"expected_revision"`
	ActualRevision          int64   `json:"actual_revision"`
	ExpectedSHA256          string  `json:"expected_logical_sha256"`
	ActualSHA256            string  `json:"actual_logical_sha256"`
	OperationErrors         int     `json:"operation_errors"`
	Recorder                Process `json:"recorder"`
	Analyzer                Process `json:"analyzer"`
}

func measure(cmd *exec.Cmd, started time.Time) Process {
	p := Process{WallNS: int64(time.Since(started))}
	if cmd.ProcessState == nil {
		return p
	}
	p.UserCPUSeconds = cmd.ProcessState.UserTime().Seconds()
	p.SystemCPUSeconds = cmd.ProcessState.SystemTime().Seconds()
	if r, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		p.PeakRSSBytes = r.Maxrss
		if runtime.GOOS != "darwin" {
			p.PeakRSSBytes *= 1024
		}
	}
	return p
}

func Run(ctx context.Context, cfg Config) (*Summary, error) {
	if cfg.Duration <= 0 || cfg.BytesPerSecond <= 0 || cfg.ValueBytes < 32 || cfg.Keys < 1 {
		return nil, fmt.Errorf("positive duration/rate/keys and value size >=32 required")
	}
	if cfg.Workload == "" {
		cfg.Workload = "raw"
	}
	if cfg.Workload != "raw" && cfg.Workload != "kubernetes" {
		return nil, fmt.Errorf("workload must be raw or kubernetes")
	}
	if cfg.CompactionInterval < 0 {
		return nil, fmt.Errorf("compaction interval must be positive")
	}
	if cfg.CompactionInterval == 0 {
		cfg.CompactionInterval = 5 * time.Second
		if cfg.Workload == "kubernetes" {
			cfg.CompactionInterval = 45 * time.Second
		}
	}
	if cfg.Workload == "kubernetes" && cfg.ValueBytes < 2048 {
		return nil, fmt.Errorf("kubernetes workload value size must be at least 2048 bytes")
	}
	if err := os.Mkdir(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	sourceDir := filepath.Join(cfg.Dir, "source")
	source, err := fixture.Start(ctx, sourceDir)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	captureDir := filepath.Join(cfg.Dir, "capture")
	recLog, err := os.Create(filepath.Join(cfg.Dir, "recorder.stdout.json"))
	if err != nil {
		return nil, err
	}
	defer recLog.Close()
	recErr, err := os.Create(filepath.Join(cfg.Dir, "recorder.stderr.log"))
	if err != nil {
		return nil, err
	}
	defer recErr.Close()
	rec := exec.CommandContext(ctx, cfg.Recorder, "--wal-dir", source.WALDir, "--snapshot", source.BaseDB, "--out", captureDir, "--poll-interval", "100ms", "--etcd-version", "3.6.0")
	rec.Stdout = recLog
	stderr, err := rec.StderrPipe()
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{}, 1)
	drained := make(chan struct{})
	if err := rec.Start(); err != nil {
		return nil, err
	}
	recStarted := time.Now()
	waited := false
	defer func() {
		if !waited {
			_ = rec.Process.Kill()
			<-drained
			_ = rec.Wait()
		}
	}()
	go func() {
		defer close(drained)
		s := bufio.NewScanner(stderr)
		for s.Scan() {
			line := s.Text()
			fmt.Fprintln(recErr, line)
			if strings.HasPrefix(line, "recording ready:") {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		}
	}()
	select {
	case <-ready:
	case <-drained:
		return nil, fmt.Errorf("recorder exited before ready; see recorder.stderr.log")
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("recorder readiness timed out")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	summary := &Summary{Workload: cfg.Workload, RecordOnly: cfg.RecordOnly, SourceVersion: "3.6.0", Platform: runtime.GOOS + "/" + runtime.GOARCH, TargetBytesPerSecond: cfg.BytesPerSecond, ValueBytes: cfg.ValueBytes, Keys: cfg.Keys}
	started := time.Now()
	if cfg.Workload == "kubernetes" {
		if err := runKubernetes(ctx, source.Client, cfg, summary, started); err != nil {
			return nil, err
		}
	} else {
		nextCompact := cfg.CompactionInterval
		for time.Since(started) < cfg.Duration {
			n := summary.Updates
			value := fmt.Sprintf("%016d", n) + strings.Repeat(string(rune('a'+n%26)), cfg.ValueBytes-16)
			response, err := source.Client.Put(ctx, fmt.Sprintf("/registry/load/default/key-%06d", n%cfg.Keys), value)
			if err != nil {
				return nil, err
			}
			summary.Updates++
			summary.ValueBytesWritten += int64(cfg.ValueBytes)
			if time.Since(started) >= nextCompact {
				if _, err := source.Client.Compact(ctx, response.Header.Revision-1, clientv3.WithCompactPhysical()); err != nil {
					return nil, err
				}
				nextCompact += cfg.CompactionInterval
			}
			due := time.Duration(float64(summary.ValueBytesWritten) / float64(cfg.BytesPerSecond) * float64(time.Second))
			if delay := due - time.Since(started); delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return nil, ctx.Err()
				}
			}
		}
	}
	summary.WorkloadNS = int64(time.Since(started))
	final, err := source.Client.Get(ctx, "\x00", clientv3.WithFromKey())
	if err != nil {
		return nil, err
	}
	summary.ExpectedRevision = final.Header.Revision
	digest := etcdutil.NewKVDigester()
	for _, kv := range final.Kvs {
		if err := digest.Add(kv); err != nil {
			return nil, err
		}
	}
	summary.ExpectedSHA256 = digest.Sum()
	source.Close()
	// Recorder's final scan follows graceful shutdown of this synthetic member.
	if err := rec.Process.Signal(os.Interrupt); err != nil {
		return nil, err
	}
	select {
	case <-drained:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("recorder finalization timed out")
	}
	err = rec.Wait()
	waited = true
	summary.Recorder = measure(rec, recStarted)
	if err != nil {
		return nil, fmt.Errorf("recorder: %w", err)
	}
	if err := os.Rename(sourceDir, filepath.Join(cfg.Dir, "source-stopped")); err != nil {
		return nil, err
	}
	manifest, err := capture.ReadManifest(captureDir)
	if err != nil {
		return nil, err
	}
	for _, w := range manifest.WALFiles {
		summary.WALBytes += w.Size
	}
	summary.WALSegments = len(manifest.WALFiles)
	summary.WALBytesPerSecond = float64(summary.WALBytes) / time.Duration(summary.WorkloadNS).Seconds()
	if cfg.RecordOnly {
		data, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(cfg.Dir, "summary.json"), append(data, '\n'), 0600); err != nil {
			return nil, err
		}
		return summary, nil
	}
	anLog, err := os.Create(filepath.Join(cfg.Dir, "analyzer.stdout.json"))
	if err != nil {
		return nil, err
	}
	defer anLog.Close()
	anErr, err := os.Create(filepath.Join(cfg.Dir, "analyzer.stderr.log"))
	if err != nil {
		return nil, err
	}
	defer anErr.Close()
	output := filepath.Join(cfg.Dir, "replay")
	an := exec.CommandContext(ctx, cfg.Analyzer, "--capture", captureDir, "--out", output, "--speed", "max", "--index-dir", filepath.Join(cfg.Dir, "index"), "--compaction-batch-limit", "16", "--compaction-sleep", "1ms")
	an.Stdout, an.Stderr = anLog, anErr
	anStarted := time.Now()
	if err := an.Run(); err != nil {
		return nil, fmt.Errorf("analyzer: %w; see analyzer.stderr.log", err)
	}
	summary.Analyzer = measure(an, anStarted)
	data, err := os.ReadFile(filepath.Join(cfg.Dir, "analyzer.stdout.json"))
	if err != nil {
		return nil, err
	}
	var info struct {
		PrepareNS   int64 `json:"prepare_duration_ns"`
		IndexBytes  int64 `json:"index_bytes"`
		AnchorBytes int64 `json:"anchor_index_bytes"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	summary.PrepareNS, summary.IndexBytes, summary.AnchorBytes = info.PrepareNS, info.IndexBytes, info.AnchorBytes
	data, err = os.ReadFile(filepath.Join(output, "report.json"))
	if err != nil {
		return nil, err
	}
	var result replay.Result
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	summary.ActualRevision, summary.ActualSHA256, summary.OperationErrors = result.FinalRevision, result.LogicalSHA256, result.OperationErrorCount
	summary.ReplayNS = result.WallDurationNS
	summary.ReplayWALBytesPerSecond = float64(summary.WALBytes) / time.Duration(summary.ReplayNS).Seconds()
	if summary.ExpectedRevision != summary.ActualRevision || summary.ExpectedSHA256 != summary.ActualSHA256 || summary.OperationErrors != 0 {
		return summary, fmt.Errorf("replay logical state mismatch or operation errors")
	}
	data, err = json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, "summary.json"), append(data, '\n'), 0600); err != nil {
		return nil, err
	}
	return summary, nil
}
