package explore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	bolt "go.etcd.io/bbolt"
)

func fixture(t *testing.T) string {
	t.Helper()
	run := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(run, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := analysis.New(analysis.Config{OutputDir: filepath.Join(run, "analysis"), Window: time.Second, TopN: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	key := func(n string) []byte { return []byte("/registry/pods/ns/" + n) }
	event := func(kind string, k []byte, rev, record, value int64, at time.Duration, tomb bool) {
		t.Helper()
		if err := a.Event(analysis.Event{Kind: kind, Key: k, Revision: rev, RecordBytes: record, ValueBytes: value, VirtualNS: int64(at), Index: uint64(rev), Tombstone: tomb}); err != nil {
			t.Fatal(err)
		}
	}
	for i, n := range []string{"a", "b", "c"} {
		event("baseline", key(n), int64(i+1), 30, 10, 0, false)
	}
	event("baseline", []byte("/registry/configmaps/ns/quiet"), 4, 220, 200, 0, false)
	if err := a.Start(analysis.Commit{DBSize: 1024, DBSizeInUse: 1024}); err != nil {
		t.Fatal(err)
	}
	event("put", key("a"), 5, 120, 100, 100*time.Millisecond, false)
	event("put", key("b"), 6, 80, 60, 200*time.Millisecond, false)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(500 * time.Millisecond), DBSize: 2048, DBSizeInUse: 1800, PendingPages: 10}); err != nil {
		t.Fatal(err)
	}
	event("put", key("b"), 7, 80, 60, 1100*time.Millisecond, false)
	event("put", key("c"), 8, 120, 100, 1200*time.Millisecond, false)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(1500 * time.Millisecond), DBSize: 4096, DBSizeInUse: 3500, PendingPages: 10}); err != nil {
		t.Fatal(err)
	}
	event("delete", key("a"), 9, 20, 0, 2100*time.Millisecond, true)
	event("compact", key("a"), 1, 30, 10, 2200*time.Millisecond, false)
	event("compact", key("a"), 5, 120, 100, 2200*time.Millisecond, false)
	event("compact", key("a"), 9, 20, 0, 2200*time.Millisecond, true)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(2500 * time.Millisecond), DBSize: 4096, DBSizeInUse: 3600, PendingPages: 20}); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(analysis.Commit{VirtualNS: int64(2600 * time.Millisecond), DBSize: 4096, DBSizeInUse: 3000, PendingPages: 5}); err != nil {
		t.Fatal(err)
	}
	summary, err := a.Finish(int64(3400 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	writeReport(t, run, summary)
	return run
}

func writeReport(t *testing.T, run string, s analysis.Summary) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"analysis_directory": "analysis", "analysis": s, "source_version": "3.4.45", "engine": "etcd-3.6.0-replay-v3", "final_revision": 9, "logical_sha256": strings.Repeat("a", 64), "batch_interval_ns": 100000000, "batch_limit": 10000})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(run, "report.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		h := sha256.Sum256(b)
		rel, _ := filepath.Rel(root, path)
		result[rel] = hex.EncodeToString(h[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestIntervalRankingUsesAllRowsAndNetAcrossRange(t *testing.T) {
	run := fixture(t)
	before := snapshot(t, run)
	s, err := Open(context.Background(), run, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Info().WindowCount != 4 || s.Info().ObjectCount != 4 {
		t.Fatalf("wrong info: %+v", s.Info())
	}
	r, err := s.Rankings(context.Background(), Query{From: 0, To: 2, Scope: "object", Metric: "writes", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Stored top-1 windows contain a then c; b is the interval leader.
	if r.Total != 3 || len(r.Items) != 1 || r.Items[0].Identity.Name != "b" || r.Items[0].Bytes != 120 || r.DenominatorBytes != 320 || r.Items[0].SharePercent != 37.5 {
		t.Fatalf("incorrect complete-range ranking: %+v", r)
	}
	r, err = s.Rankings(context.Background(), Query{From: 0, To: 3, Scope: "object", Metric: "history", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 2 || r.Items[0].Identity.Name != "b" || r.Items[0].Bytes != 110 || r.DenominatorBytes != 140 {
		t.Fatalf("window positives incorrectly summed: %+v", r)
	}
	for _, i := range r.Items {
		if i.Identity.Name == "a" {
			t.Fatal("fully removed history ranked as positive interval growth")
		}
	}
	r, err = s.Rankings(context.Background(), Query{From: 0, To: 3, Scope: "resource", Metric: "history", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 1 || r.Items[0].Identity.Resource != "pods" || r.Items[0].Bytes != 140 {
		t.Fatalf("bad resource interval sum: %+v", r)
	}
	r, err = s.Rankings(context.Background(), Query{From: 0, To: 2, Scope: "object", Metric: "writes", Resource: "pods", Namespace: "ns", Search: "/b", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 1 || r.DenominatorBytes != 120 || r.Items[0].SharePercent != 100 {
		t.Fatal("filter denominator incorrect")
	}
	if after := snapshot(t, run); !reflect.DeepEqual(before, after) {
		t.Fatal("exploration changed source artifacts")
	}
}

func TestObjectCarryQuietBaselineAndDeletedFinal(t *testing.T) {
	s, err := Open(context.Background(), fixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	list, err := s.Objects(context.Background(), "quiet", "configmaps", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 {
		t.Fatal("baseline-only quiet object missing")
	}
	d, err := s.Object(context.Background(), list.Items[0].ID, 1, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if d.Start.LiveValueBytes != 200 || d.End != d.Start || d.Metrics != (analysis.Metrics{}) || len(d.Points) != 4 {
		t.Fatalf("quiet baseline was not carried: %+v", d)
	}
	for _, p := range d.Points {
		if p.Stock != d.Start {
			t.Fatal("quiet stock reset to zero")
		}
	}
	list, err = s.Objects(context.Background(), "/a", "pods", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	d, err = s.Object(context.Background(), list.Items[0].ID, 1, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if d.Baseline.RetainedBytes != 30 || d.Start.RetainedBytes != 150 || d.End.RetainedBytes != 0 || d.Final.RetainedBytes != 0 || d.Metrics.Deletes != 1 || d.Metrics.CompactedRecords != 3 {
		t.Fatalf("deleted object resurrected or wrong carry: %+v", d)
	}
	if d.Points[1].Stock.RetainedBytes != 150 || d.Points[2].Stock.RetainedBytes != 0 || d.Points[3].Stock.RetainedBytes != 0 {
		t.Fatal("sparse history gaps wrong")
	}
	if _, err = s.Object(context.Background(), strings.Repeat("f", 64), 0, 4, 16); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown object accepted")
	}
}

func TestCurvePreservesExtremaWhileBounded(t *testing.T) {
	run := filepath.Join(t.TempDir(), "run")
	os.Mkdir(run, 0700)
	a, err := analysis.New(analysis.Config{OutputDir: filepath.Join(run, "analysis"), Window: time.Second, TopN: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Start(analysis.Commit{DBSize: 1000, DBSizeInUse: 500}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		size, inuse := int64(1000), int64(500)
		if i == 47 {
			size, inuse = 10000, 9990
		}
		if i == 77 {
			inuse = 50
		}
		if err = a.Commit(analysis.Commit{VirtualNS: int64(time.Duration(i)*time.Second + 500*time.Millisecond), DBSize: size, DBSizeInUse: inuse}); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := a.Finish(int64(99900 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	writeReport(t, run, sum)
	s, err := Open(context.Background(), run, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	points, err := s.Curve(context.Background(), 0, 100, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) > 16 || points[0].Window != -1 || points[len(points)-1].Window != 99 {
		t.Fatalf("unbounded or missing endpoints: %+v", points)
	}
	var peakDB, peakRatio bool
	for _, p := range points {
		peakDB = peakDB || (p.Window == 47 && p.DBSize == 10000)
		peakRatio = peakRatio || (p.Window == 77 && p.FragmentationPercent == 95)
	}
	if !peakDB || !peakRatio {
		t.Fatal("downsampling hid extrema")
	}
}

func TestInputValidationCancellationAndCacheIsolation(t *testing.T) {
	t.Run("corrupt rows", func(t *testing.T) {
		run := fixture(t)
		f, err := os.OpenFile(filepath.Join(run, "analysis", "objects.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("{}\n")
		f.Close()
		if s, err := Open(context.Background(), run, ""); err == nil {
			s.Close()
			t.Fatal("accepted malformed rows")
		}
	})
	t.Run("checksum", func(t *testing.T) {
		run := fixture(t)
		p := filepath.Join(run, "analysis", "windows.jsonl")
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, ' ')
		os.WriteFile(p, b, 0600)
		if s, err := Open(context.Background(), run, ""); err == nil {
			s.Close()
			t.Fatal("accepted checksum change")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		run := fixture(t)
		p := filepath.Join(run, "analysis", "objects.jsonl")
		outside := filepath.Join(t.TempDir(), "objects.jsonl")
		if err := os.Rename(p, outside); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, p); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(context.Background(), run, ""); err == nil {
			s.Close()
			t.Fatal("accepted external artifact")
		}
	})
	t.Run("cache", func(t *testing.T) {
		run := fixture(t)
		before := snapshot(t, run)
		if s, err := Open(context.Background(), run, filepath.Join(run, "cache")); err == nil {
			s.Close()
			t.Fatal("accepted cache inside source")
		}
		t.Setenv("TMPDIR", run)
		if s, err := Open(context.Background(), run, ""); err == nil {
			s.Close()
			t.Fatal("accepted temporary cache inside source")
		}
		if !reflect.DeepEqual(before, snapshot(t, run)) {
			t.Fatal("invalid cache request changed source")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		run := fixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := Open(ctx, run, ""); !errors.Is(err, context.Canceled) {
			t.Fatal("cancel not propagated")
		}
		s, err := Open(context.Background(), run, "")
		if err != nil {
			t.Fatal(err)
		}
		cache := s.cache
		if _, err = s.Rankings(ctx, Query{From: 0, To: 4, Scope: "object", Metric: "writes", Limit: 10}); !errors.Is(err, context.Canceled) {
			t.Fatal("query cancel not propagated")
		}
		s.Close()
		if _, err = os.Stat(cache); !os.IsNotExist(err) {
			t.Fatal("temporary index not removed")
		}
	})
}

func TestConcurrentQueriesAndPagination(t *testing.T) {
	s, err := Open(context.Background(), fixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Rankings(context.Background(), Query{From: 0, To: 2, Scope: "object", Metric: "writes", Limit: 1, Offset: 1})
			if err != nil {
				t.Error(err)
				return
			}
			if len(r.Items) != 1 || r.Total != 3 || r.DenominatorBytes != 320 {
				t.Error("incorrect page")
			}
		}()
	}
	wg.Wait()
}

func TestRetainedCacheAndFailedInitialization(t *testing.T) {
	run := fixture(t)
	cache := filepath.Join(t.TempDir(), "cache")
	s, err := Open(context.Background(), run, cache)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(cache, "index.db")); err != nil {
		t.Fatal("explicit cache did not persist", err)
	}
	before := snapshot(t, cache)
	if again, err := Open(context.Background(), run, cache); err == nil {
		again.Close()
		t.Fatal("existing cache was silently reused")
	}
	if !reflect.DeepEqual(before, snapshot(t, cache)) {
		t.Fatal("rejected cache was modified")
	}
	path := filepath.Join(t.TempDir(), "failed.db")
	if _, err = newWriter(path, []byte("same"), []byte("same")); err == nil {
		t.Fatal("duplicate buckets accepted")
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal("failed initialization left DB locked", err)
	}
	db.Close()
}
