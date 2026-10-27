package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExactHistoryAndEstimatedPhysicalAttribution(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{OutputDir: dir, Window: time.Second, TopN: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	hot := []byte("/registry/configmaps/default/hot")
	cold := []byte("/registry/secrets/default/cold")
	event := func(kind string, key []byte, rev, record, value int64, at time.Duration, tomb bool) {
		t.Helper()
		if err := a.Event(Event{Kind: kind, Key: key, Revision: rev, RecordBytes: record, ValueBytes: value, Tombstone: tomb, VirtualNS: int64(at), Index: uint64(rev)}); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(at time.Duration, size, inuse int64) {
		t.Helper()
		if err := a.Commit(Commit{VirtualNS: int64(at), DBSize: size, DBSizeInUse: inuse}); err != nil {
			t.Fatal(err)
		}
	}
	event("baseline", hot, 1, 100, 50, 0, false)
	event("baseline", hot, 2, 120, 60, 0, false)
	event("baseline", cold, 3, 500, 400, 0, false)
	if err := a.Start(Commit{DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	event("put", hot, 4, 140, 80, 100*time.Millisecond, false)
	event("put", cold, 5, 100, 50, 300*time.Millisecond, false)
	commit(500*time.Millisecond, 8192, 6144)
	event("compact", hot, 1, 100, 50, 1200*time.Millisecond, false)
	event("compact", hot, 2, 120, 60, 1200*time.Millisecond, false)
	if err = a.Commit(Commit{VirtualNS: int64(1250 * time.Millisecond), DBSize: 8192, DBSizeInUse: 6144, PendingPages: 16}); err != nil {
		t.Fatal(err)
	}
	commit(1300*time.Millisecond, 8192, 4096)
	event("delete", cold, 6, 50, 0, 1500*time.Millisecond, true)
	commit(1600*time.Millisecond, 8192, 5000)
	event("compact", cold, 3, 500, 400, 2100*time.Millisecond, false)
	event("compact", cold, 5, 100, 50, 2100*time.Millisecond, false)
	event("compact", cold, 6, 50, 0, 2100*time.Millisecond, true)
	if err = a.Commit(Commit{VirtualNS: int64(2150 * time.Millisecond), DBSize: 8192, DBSizeInUse: 5000, PendingPages: 16}); err != nil {
		t.Fatal(err)
	}
	commit(2200*time.Millisecond, 8192, 4096)
	commit(2500*time.Millisecond, 8192, 3584)
	s, err := a.Finish(int64(2800 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if s.Initial.RetainedBytes != 720 || s.Initial.HistoryBytes != 100 || s.Final.RetainedBytes != 140 || s.Final.HistoryBytes != 0 || s.Final.LiveValueBytes != 80 || s.Final.LiveObjects != 1 {
		t.Fatalf("incorrect stocks: %+v -> %+v", s.Initial, s.Final)
	}
	if s.Metrics.RecordWriteBytes != 290 || s.Metrics.ValueWriteBytes != 130 || s.Metrics.SupersededLiveBytes != 720 || s.Metrics.CompactedBytes != 870 || s.Metrics.Puts != 2 || s.Metrics.Deletes != 1 {
		t.Fatalf("incorrect work accounting: %+v", s.Metrics)
	}
	if s.DBGrowthBytes != 4096 || s.Metrics.EstimatedDBGrowthBytes != 4096 || s.FreeGrowthBytes != 3464 || s.Metrics.EstimatedFreeGrowthBytes != 2952 || s.UnattributedFreeGrowthBytes != 512 || s.FreeReuseBytes != 904 || s.Metrics.EstimatedFreeReuseBytes != 904 {
		t.Fatalf("incorrect physical attribution: %+v", s)
	}
	windows := readJSONL[Window](t, filepath.Join(dir, "windows.jsonl"))
	if len(windows) != 3 || windows[0].End.HistoryBytes != 720 || windows[1].End.HistoryBytes != 650 || windows[2].End.HistoryBytes != 0 {
		t.Fatalf("incorrect windows: %+v", windows)
	}
	if windows[0].Objects.HistoryGrowth[0].Identity.Name != "cold" || windows[0].Objects.HistoryGrowth[0].Bytes != 500 {
		t.Fatalf("wrong history ranking: %+v", windows[0].Objects.HistoryGrowth)
	}
	if windows[1].Objects.FreeGrowth[0].Identity.Name != "hot" || windows[1].Objects.FreeGrowth[0].Bytes != 2048 {
		t.Fatalf("wrong compaction/free attribution: %+v", windows[1].Objects.FreeGrowth)
	}
	for _, w := range windows {
		if w.DBGrowthBytes != w.Metrics.EstimatedDBGrowthBytes+w.UnattributedDBGrowthBytes || w.FreeGrowthBytes != w.Metrics.EstimatedFreeGrowthBytes+w.UnattributedFreeGrowthBytes || w.FreeReuseBytes != w.Metrics.EstimatedFreeReuseBytes+w.UnattributedFreeReuseBytes {
			t.Fatalf("unreconciled window: %+v", w)
		}
	}
	rows := readJSONL[Row](t, filepath.Join(dir, "objects.jsonl"))
	if len(rows) != 5 {
		t.Fatalf("want five sparse object rows, got%d", len(rows))
	}
	for _, r := range rows {
		if r.Identity.Name == "cold" && r.WindowStartNS == int64(2*time.Second) && r.End.RetainedBytes != 0 {
			t.Fatal("deleted object still retains records")
		}
	}
	resources := readJSONL[Row](t, filepath.Join(dir, "resources.jsonl"))
	var byObject, byResource Metrics
	for _, r := range rows {
		addMetrics(&byObject, r.Metrics)
	}
	for _, r := range resources {
		addMetrics(&byResource, r.Metrics)
	}
	if byObject != s.Metrics || byResource != s.Metrics {
		t.Fatalf("object/resource totals do not reconcile: %+v / %+v / %+v", byObject, byResource, s.Metrics)
	}
}

func TestCrossWindowCommitWeightsAndQuietStockCarry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{OutputDir: dir, Window: time.Second, TopN: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []Event{{Kind: "put", Key: []byte("/registry/pods/ns/a"), Revision: 1, RecordBytes: 100, ValueBytes: 10, VirtualNS: int64(900 * time.Millisecond), Index: 1}, {Kind: "put", Key: []byte("/registry/pods/ns/b"), Revision: 2, RecordBytes: 300, ValueBytes: 20, VirtualNS: int64(1100 * time.Millisecond), Index: 2}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Commit(Commit{VirtualNS: int64(1200 * time.Millisecond), DBSize: 2000, DBSizeInUse: 1900}); err != nil {
		t.Fatal(err)
	}
	s, err := a.Finish(int64(3500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if s.Metrics.EstimatedDBGrowthBytes != 1000 || s.UnattributedFreeGrowthBytes != 100 {
		t.Fatalf("bad estimates: %+v", s)
	}
	w := readJSONL[Window](t, filepath.Join(dir, "windows.jsonl"))
	if len(w) != 4 || w[0].End.RetainedBytes != 100 || w[1].Start.RetainedBytes != 100 || w[1].End.RetainedBytes != 400 || w[2].End.RetainedBytes != 400 || w[3].End.RetainedBytes != 400 {
		t.Fatalf("bad stock carry: %+v", w)
	}
	if w[0].Metrics.EstimatedDBGrowthBytes != 0 || w[1].Objects.DBGrowth[0].Identity.Name != "b" || w[1].Objects.DBGrowth[0].Bytes != 750 {
		t.Fatalf("weights not attributed at commit: %+v", w[1])
	}
	rows := readJSONL[Row](t, filepath.Join(dir, "objects.jsonl"))
	var aInSecond *Row
	for i := range rows {
		if rows[i].WindowStartNS == int64(time.Second) && rows[i].Identity.Name == "a" {
			aInSecond = &rows[i]
		}
	}
	if aInSecond == nil || aInSecond.Metrics.EstimatedDBGrowthBytes != 250 || aInSecond.Metrics.Puts != 0 || aInSecond.Start.RetainedBytes != 100 || aInSecond.End.RetainedBytes != 100 || aInSecond.FirstIndex != 1 {
		t.Fatalf("missing deferred attribution row: %+v", aInSecond)
	}
	if len(w[2].Objects.ValueWrites) != 0 || len(w[3].Objects.DBGrowth) != 0 {
		t.Fatal("quiet window acquired activity")
	}
}

func TestBaselineTombstonesAndRecreatedObject(t *testing.T) {
	a, err := New(Config{OutputDir: filepath.Join(t.TempDir(), "analysis")})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	key := []byte("/registry/pods/ns/deleted")
	for _, e := range []Event{{Kind: "baseline", Key: key, Revision: 1, RecordBytes: 100, ValueBytes: 50}, {Kind: "baseline", Key: key, Revision: 2, RecordBytes: 25, Tombstone: true}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	if a.stock.LiveObjects != 0 || a.stock.HistoryBytes != 125 {
		t.Fatalf("bad tombstone baseline: %+v", a.stock)
	}
	for _, e := range []Event{{Kind: "compact", Key: key, Revision: 1, RecordBytes: 100, ValueBytes: 50}, {Kind: "compact", Key: key, Revision: 2, RecordBytes: 25, Tombstone: true}, {Kind: "put", Key: key, Revision: 3, RecordBytes: 200, ValueBytes: 120}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Commit(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	s, err := a.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Final.RetainedBytes != 200 || s.Final.LiveValueBytes != 120 || s.Final.HistoryBytes != 0 || s.Final.LiveObjects != 1 {
		t.Fatalf("bad recreated state: %+v", s.Final)
	}
}

func TestRejectsUncommittedEndAndLiveCompaction(t *testing.T) {
	a, err := New(Config{OutputDir: filepath.Join(t.TempDir(), "analysis")})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	e := Event{Kind: "put", Key: []byte("key"), Revision: 1, RecordBytes: 100, ValueBytes: 50}
	if err = a.Event(e); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Finish(0); err == nil {
		t.Fatal("accepted uncommitted end")
	}
	e.Kind = "compact"
	if err = a.Event(e); err == nil {
		t.Fatal("accepted removal of latest live record")
	}
}

func TestLargeIntegerAllocationReconciles(t *testing.T) {
	const amount int64 = 1_209_600_000_000
	const total int64 = 1_000_000_000_000
	a := share(amount, 333_333_333_333, total)
	b := share(amount, 777_777_777_777, total) - a
	c := share(amount, total, total) - a - b
	if a+b+c != amount || a <= 0 || b <= 0 || c <= 0 {
		t.Fatalf("large allocation overflowed: %d %d %d", a, b, c)
	}
}

func TestNetFreeRankingAccountsForReuse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{OutputDir: dir, Window: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	keyA, keyB := []byte("/registry/configmaps/ns/a"), []byte("/registry/secrets/ns/b")
	for _, e := range []Event{{Kind: "baseline", Key: keyA, Revision: 1, RecordBytes: 200}, {Kind: "baseline", Key: keyA, Revision: 2, RecordBytes: 10}, {Kind: "baseline", Key: keyB, Revision: 3, RecordBytes: 100}, {Kind: "baseline", Key: keyB, Revision: 4, RecordBytes: 10}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Start(Commit{DBSize: 2000, DBSizeInUse: 1800}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []Event{{Kind: "compact", Key: keyA, Revision: 1, RecordBytes: 200}, {Kind: "compact", Key: keyB, Revision: 3, RecordBytes: 100}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Commit(Commit{DBSize: 2000, DBSizeInUse: 1800, PendingPages: 10}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 2000, DBSizeInUse: 900}); err != nil {
		t.Fatal(err)
	}
	if err = a.Event(Event{Kind: "put", Key: keyA, Revision: 5, RecordBytes: 600}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 2000, DBSizeInUse: 1400}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Finish(int64(time.Second / 2)); err != nil {
		t.Fatal(err)
	}
	w := readJSONL[Window](t, filepath.Join(dir, "windows.jsonl"))[0]
	if w.Objects.FreeGrowth[0].Identity.Name != "a" || w.Objects.FreeGrowth[0].Bytes != 600 {
		t.Fatalf("incorrect gross free ranking: %+v", w.Objects.FreeGrowth)
	}
	if w.Objects.FreeNetGrowth[0].Identity.Name != "b" || w.Objects.FreeNetGrowth[0].Bytes != 300 || w.Objects.FreeNetGrowthBytes != 400 {
		t.Fatalf("reuse did not change net ranking: %+v", w.Objects)
	}
	if w.Resources.FreeNetGrowth[0].Identity.Resource != "secrets" || w.Resources.FreeNetGrowthBytes != 400 {
		t.Fatalf("incorrect resource net ranking: %+v", w.Resources)
	}
}

func TestCancellationDoesNotPublishSummary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{Context: ctx, OutputDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = a.Finish(int64(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	if _, err = os.Stat(filepath.Join(dir, "summary.json")); !os.IsNotExist(err) {
		t.Fatalf("canceled analysis published summary: %v", err)
	}
}

func TestDeferredCompactionReleaseCrossesWindowAndExpires(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{OutputDir: dir, Window: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	keyA, keyB := []byte("/registry/configmaps/ns/a"), []byte("/registry/secrets/ns/b")
	for _, e := range []Event{{Kind: "baseline", Key: keyA, Revision: 1, RecordBytes: 200}, {Kind: "baseline", Key: keyA, Revision: 2, RecordBytes: 10}, {Kind: "baseline", Key: keyB, Revision: 3, RecordBytes: 100}, {Kind: "baseline", Key: keyB, Revision: 4, RecordBytes: 10}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Start(Commit{DBSize: 10000, DBSizeInUse: 10000}); err != nil {
		t.Fatal(err)
	}
	// First compaction creates pending pages; it does not yet publish free space.
	if err = a.Event(Event{Kind: "compact", Key: keyA, Revision: 1, RecordBytes: 200, Index: 10, VirtualNS: int64(900 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 10000, DBSizeInUse: 10000, PendingPages: 8, VirtualNS: int64(900 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	// Another compaction in the next window can create even more pending pages;
	// old releases still belong to A, not B and not a pending-page net decline.
	if err = a.Event(Event{Kind: "compact", Key: keyB, Revision: 3, RecordBytes: 100, Index: 11, VirtualNS: int64(1100 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 10000, DBSizeInUse: 9200, PendingPages: 12, VirtualNS: int64(1100 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err = a.Event(Event{Kind: "put", Key: keyA, Revision: 5, RecordBytes: 300, Index: 12, VirtualNS: int64(1200 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 10000, DBSizeInUse: 8800, PendingPages: 2, VirtualNS: int64(1200 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	// Writes-only pending work cannot inherit B's prior compaction association.
	if err = a.Commit(Commit{DBSize: 10000, DBSizeInUse: 8700, PendingPages: 1, VirtualNS: int64(1300 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	s, err := a.Finish(int64(1500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if s.FreeGrowthBytes != 1300 || s.Metrics.EstimatedFreeGrowthBytes != 1200 || s.Metrics.EstimatedDeferredFreeGrowthBytes != 1200 || s.UnattributedFreeGrowthBytes != 100 {
		t.Fatalf("incorrect deferred reconciliation: %+v", s)
	}
	w := readJSONL[Window](t, filepath.Join(dir, "windows.jsonl"))
	if len(w) != 2 || w[0].Metrics.CompactedBytes != 200 || w[0].Metrics.EstimatedFreeGrowthBytes != 0 || w[1].Metrics.CompactedBytes != 100 || w[1].Metrics.EstimatedFreeGrowthBytes != 1200 {
		t.Fatalf("wrong release-window attribution: %+v", w)
	}
	rows := readJSONL[Row](t, filepath.Join(dir, "objects.jsonl"))
	var secondA, secondB Row
	for _, r := range rows {
		if r.WindowStartNS == int64(time.Second) {
			if r.Identity.Name == "a" {
				secondA = r
			}
			if r.Identity.Name == "b" {
				secondB = r
			}
		}
	}
	if secondA.Metrics.EstimatedDeferredFreeGrowthBytes != 800 || secondA.FirstIndex != 10 || secondA.Metrics.CompactedBytes != 0 || secondB.Metrics.EstimatedDeferredFreeGrowthBytes != 400 {
		t.Fatalf("release was credited to current work: a=%+v b=%+v", secondA, secondB)
	}
}

func TestPendingReleaseWeightsExpireWithoutFreeGrowth(t *testing.T) {
	a, err := New(Config{OutputDir: filepath.Join(t.TempDir(), "analysis")})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	key := []byte("/registry/configmaps/ns/a")
	for _, e := range []Event{{Kind: "baseline", Key: key, Revision: 1, RecordBytes: 100}, {Kind: "baseline", Key: key, Revision: 2, RecordBytes: 10}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000}); err != nil {
		t.Fatal(err)
	}
	if err = a.Event(Event{Kind: "compact", Key: key, Revision: 1, RecordBytes: 100}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 1000, DBSizeInUse: 1000, PendingPages: 4}); err != nil {
		t.Fatal(err)
	}
	// Release may be consumed immediately, producing no observed increase.
	if err = a.Commit(Commit{DBSize: 1000, DBSizeInUse: 1000, PendingPages: 2}); err != nil {
		t.Fatal(err)
	}
	if err = a.Commit(Commit{DBSize: 1000, DBSizeInUse: 900, PendingPages: 1}); err != nil {
		t.Fatal(err)
	}
	s, err := a.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metrics.EstimatedFreeGrowthBytes != 0 || s.UnattributedFreeGrowthBytes != 100 {
		t.Fatalf("stale weights survived anothercommit: %+v", s)
	}
}

func TestCurrentCompactionDoesNotClaimOlderRelease(t *testing.T) {
	a, err := New(Config{OutputDir: filepath.Join(t.TempDir(), "analysis")})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	key := []byte("/registry/configmaps/ns/a")
	for _, e := range []Event{{Kind: "baseline", Key: key, Revision: 1, RecordBytes: 100}, {Kind: "baseline", Key: key, Revision: 2, RecordBytes: 10}} {
		if err = a.Event(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Start(Commit{DBSize: 1000, DBSizeInUse: 1000, PendingPages: 2}); err != nil {
		t.Fatal(err)
	}
	if err = a.Event(Event{Kind: "compact", Key: key, Revision: 1, RecordBytes: 100}); err != nil {
		t.Fatal(err)
	}
	// Even zero current pending pages do not establish that this gain came
	// from the current compaction; it can be an old COW release.
	if err = a.Commit(Commit{DBSize: 1000, DBSizeInUse: 900, PendingPages: 0}); err != nil {
		t.Fatal(err)
	}
	s, err := a.Finish(0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Metrics.CompactedBytes != 100 || s.Metrics.EstimatedFreeGrowthBytes != 0 || s.UnattributedFreeGrowthBytes != 100 {
		t.Fatalf("current compaction claimed an older release: %+v", s)
	}
}

func readJSONL[T any](t *testing.T, path string) []T {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []T
	d := json.NewDecoder(f)
	for {
		var row T
		err = d.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}
