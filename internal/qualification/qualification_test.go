package qualification

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal"
)

func TestRecordAndReplayRealEtcdAtDifferentSpeeds(t *testing.T) {
	// Only this synthetic member uses smaller WAL files to exercise rotation.
	old := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 256 * 1024
	defer func() { wal.SegmentSizeBytes = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := Run(ctx, Config{Dir: filepath.Join(t.TempDir(), "qualification"), Workload: fixture.Workload{Updates: 96, ValueBytes: 12 * 1024, Interval: 2 * time.Millisecond}, Speeds: []float64{1, 100, 0}})
	if err != nil {
		t.Fatal(err)
	}
	if s.WALSegments < 2 {
		t.Fatalf("rotation not exercised: %d segments", s.WALSegments)
	}
	for _, kind := range []string{"txn", "put", "delete", "lease-grant", "lease-revoke", "compaction"} {
		if s.OperationCounts[kind] == 0 {
			t.Errorf("missing operation coverage: %s", kind)
		}
	}
	if !s.CommitBoundariesEqual || !s.CaptureUnchanged {
		t.Fatalf("qualification invariants failed: %+v", s)
	}
	t.Logf("source revision=%d, keys=%d, committed entries=%d, segments=%d, commits=%d, equal space curves=%v", s.SourceRevision, s.SourceKeyCount, s.CommittedEntries, s.WALSegments, s.Replays[0].Commits, s.SpaceCurvesEqual)
}
