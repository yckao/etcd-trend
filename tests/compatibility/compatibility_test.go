package compatibility_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/compatfixture"
	"github.com/yckao/etcd-trend/internal/fixture"
)

// Set these variables to explicit binaries. No download or existing cluster
// discovery happens as a side effect of running the normal test suite.
func TestExternalEtcdReleases(t *testing.T) {
	for _, tc := range []struct{ env, prefix string }{{"ETCD_TREND_ETCD_34", "3.4."}, {"ETCD_TREND_ETCD_35", "3.5."}, {"ETCD_TREND_ETCD_36", "3.6."}} {
		t.Run(tc.env, func(t *testing.T) {
			binary := os.Getenv(tc.env)
			if binary == "" {
				t.Skip("set " + tc.env + " to enable this external release fixture")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			s, err := compatfixture.Run(ctx, compatfixture.Config{Binary: binary, Dir: filepath.Join(t.TempDir(), "run"), Workload: fixture.Workload{Updates: 48, ValueBytes: 8192, Interval: time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(s.Binary.Version, tc.prefix) {
				t.Fatalf("%s must select etcd %s, got %s", tc.env, tc.prefix, s.Binary.Version)
			}
			if s.Status != "passed" || !s.CommitBoundariesEqual || !s.CaptureUnchanged || len(s.Replays) != 3 {
				t.Fatalf("incomplete qualification: %+v", s)
			}
			if s.OperationCounts["compaction"] == 0 || s.OperationCounts["lease-revoke"] == 0 || s.OperationCounts["txn"] == 0 || s.OperationCounts["delete"] == 0 {
				t.Fatalf("missing workload coverage: %+v", s.OperationCounts)
			}
		})
	}
}
