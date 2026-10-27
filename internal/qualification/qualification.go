// Package qualification exercises the complete recorder/analyzer path against
// an isolated real etcd member. It is not imported by either production CLI.
package qualification

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/internal/record"
	"github.com/yckao/etcd-trend/internal/replay"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

type Config struct {
	Dir      string
	Workload fixture.Workload
	Speeds   []float64
}

type ReplaySummary struct {
	Speed            float64 `json:"speed"`
	Applied          int     `json:"applied_entries"`
	Commits          int     `json:"commits"`
	Revision         int64   `json:"revision"`
	LogicalSHA256    string  `json:"logical_sha256"`
	WallNS           int64   `json:"wall_duration_ns"`
	VirtualNS        int64   `json:"virtual_duration_ns"`
	FinalDBSize      int64   `json:"final_db_size"`
	FinalDBSizeInUse int64   `json:"final_db_size_in_use"`
	Report           string  `json:"report"`
}

type Summary struct {
	Capture               string          `json:"capture"`
	SourceRevision        int64           `json:"source_revision"`
	SourceLogicalSHA256   string          `json:"source_logical_sha256"`
	SourceKeyCount        int             `json:"source_key_count"`
	CompactedRevision     int64           `json:"compacted_revision"`
	WALSegments           int             `json:"wal_segments"`
	CommittedEntries      int             `json:"committed_entries"`
	OperationCounts       map[string]int  `json:"operation_counts"`
	Replays               []ReplaySummary `json:"replays"`
	CommitBoundariesEqual bool            `json:"commit_boundaries_equal"`
	SpaceCurvesEqual      bool            `json:"space_curves_equal"`
	CaptureUnchanged      bool            `json:"capture_unchanged"`
}

// Run owns only files under a new directory and its temporary loopback fixture.
func Run(ctx context.Context, cfg Config) (summary Summary, err error) {
	if cfg.Dir == "" {
		return summary, fmt.Errorf("qualification directory is required")
	}
	if err = os.Mkdir(cfg.Dir, 0700); err != nil {
		return summary, err
	}
	if len(cfg.Speeds) == 0 {
		cfg.Speeds = []float64{1, 100, 0}
	}
	source, err := fixture.Start(ctx, filepath.Join(cfg.Dir, "source"))
	if err != nil {
		return summary, err
	}
	defer source.Close()
	capDir := filepath.Join(cfg.Dir, "capture")
	recCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan struct{})
	recDone := make(chan error, 1)
	go func() {
		recDone <- record.Run(recCtx, record.Config{WALDir: source.WALDir, OutputDir: capDir, SnapshotSource: source.BaseDB, PollInterval: 5 * time.Millisecond, EtcdVersion: "3.6.0", Ready: func(capture.Manifest) { close(ready) }})
	}()
	select {
	case <-ready:
	case err = <-recDone:
		return summary, fmt.Errorf("recorder startup: %w", err)
	case <-ctx.Done():
		cancel()
		<-recDone
		return summary, ctx.Err()
	}
	expected, err := source.Exercise(ctx, cfg.Workload)
	if err != nil {
		cancel()
		<-recDone
		return summary, fmt.Errorf("fixture workload: %w", err)
	}
	source.Close()
	cancel()
	if err = <-recDone; err != nil {
		return summary, fmt.Errorf("record: %w", err)
	}
	// Move the closed fixture away from the paths originally given to recorder.
	// Replay must rely exclusively on its portable capture.
	if err = os.Rename(filepath.Join(cfg.Dir, "source"), filepath.Join(cfg.Dir, "source-stopped")); err != nil {
		return summary, fmt.Errorf("isolate recorded capture: %w", err)
	}
	d, err := capture.Load(capDir)
	if err != nil {
		return summary, err
	}
	kvs := make([]mvccpb.KeyValue, len(expected.KVs))
	for i, kv := range expected.KVs {
		kvs[i] = *kv
	}
	summary = Summary{Capture: capDir, SourceRevision: expected.Revision, SourceLogicalSHA256: etcdutil.HashKVs(kvs), SourceKeyCount: len(kvs), CompactedRevision: expected.CompactedRevision, WALSegments: len(d.Manifest.WALFiles), CommittedEntries: len(d.Entries), OperationCounts: map[string]int{}, CommitBoundariesEqual: true, SpaceCurvesEqual: true}
	before, err := captureHashes(capDir, d.Manifest)
	if err != nil {
		return summary, err
	}
	var first replay.Result
	for n, speed := range cfg.Speeds {
		name := fmt.Sprintf("replay-%d", n)
		out := filepath.Join(cfg.Dir, name)
		result, runErr := replay.Run(ctx, replay.Config{BaselinePath: d.BaselinePath, SourceVersion: d.Manifest.EtcdVersion, RetainObservations: true, Entries: d.Entries, EntryTimes: d.EntryTimes, OutputDir: out, Speed: speed, BatchInterval: 100 * time.Millisecond, BatchLimit: 10000, CompactionBatchLimit: 16, CompactionSleepInterval: time.Millisecond, MemberID: d.Manifest.MemberID, ClusterID: d.Manifest.ClusterID})
		if runErr != nil {
			return summary, fmt.Errorf("replay speed %g: %w", speed, runErr)
		}
		if result.FinalRevision != expected.Revision || result.LogicalSHA256 != summary.SourceLogicalSHA256 {
			return summary, fmt.Errorf("replay speed %g diverged: revision %d vs %d, digest %s vs %s", speed, result.FinalRevision, expected.Revision, result.LogicalSHA256, summary.SourceLogicalSHA256)
		}
		if n == 0 {
			first = result
			for _, op := range result.Operations {
				summary.OperationCounts[op.Type]++
			}
		} else {
			if !sameBoundaries(first.Commits, result.Commits) {
				summary.CommitBoundariesEqual = false
				return summary, fmt.Errorf("speed %g changed modeled commit boundaries", speed)
			}
			if !reflect.DeepEqual(first.Commits, result.Commits) {
				summary.SpaceCurvesEqual = false
			}
		}
		r := ReplaySummary{Speed: speed, Applied: result.AppliedEntries, Commits: len(result.Commits), Revision: result.FinalRevision, LogicalSHA256: result.LogicalSHA256, WallNS: result.WallDurationNS, VirtualNS: result.VirtualDurationNS, Report: filepath.Join(out, "report.json")}
		if len(result.Commits) > 0 {
			last := result.Commits[len(result.Commits)-1]
			r.FinalDBSize = last.DBSize
			r.FinalDBSizeInUse = last.DBSizeInUse
		}
		summary.Replays = append(summary.Replays, r)
	}
	after, err := captureHashes(capDir, d.Manifest)
	if err != nil {
		return summary, err
	}
	summary.CaptureUnchanged = reflect.DeepEqual(before, after)
	if !summary.CaptureUnchanged {
		return summary, fmt.Errorf("replay modified its capture")
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return summary, err
	}
	err = os.WriteFile(filepath.Join(cfg.Dir, "summary.json"), append(data, '\n'), 0600)
	return summary, err
}

func sameBoundaries(a, b []replay.Commit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].VirtualNS != b[i].VirtualNS || a[i].Reason != b[i].Reason {
			return false
		}
	}
	return true
}

func captureHashes(dir string, m capture.Manifest) (map[string]string, error) {
	names := []string{"manifest.json", "baseline.db", "anchors.jsonl", "journal.jsonl"}
	for _, w := range m.WALFiles {
		names = append(names, filepath.Join("wal", w.Name))
	}
	r := map[string]string{}
	for _, name := range names {
		hash, err := capture.FileSHA256(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		r[name] = hash
	}
	return r, nil
}
