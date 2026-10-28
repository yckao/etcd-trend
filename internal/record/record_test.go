package record_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/record"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal/walpb"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func baseline(t *testing.T, path string, index uint64) {
	t.Helper()
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		m, err := tx.CreateBucket([]byte("meta"))
		if err != nil {
			return err
		}
		if _, err = tx.CreateBucket([]byte("key")); err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], index)
		if err = m.Put([]byte("consistent_index"), value[:]); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(value[:], 1)
		return m.Put([]byte("term"), value[:])
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func newWAL(t *testing.T, dir string) *wal.WAL {
	t.Helper()
	md := etcdserverpb.Metadata{NodeID: 11, ClusterID: 22}
	b, _ := md.Marshal()
	w, err := wal.Create(zap.NewNop(), dir, b)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func save(t *testing.T, w *wal.WAL, term, commit uint64, entries ...raftpb.Entry) {
	t.Helper()
	if err := w.Save(raftpb.HardState{Term: term, Commit: commit}, entries); err != nil {
		t.Fatal(err)
	}
}

func entry(index, term uint64, value string) raftpb.Entry {
	r := etcdserverpb.InternalRaftRequest{Put: &etcdserverpb.PutRequest{Key: []byte("k"), Value: []byte(value)}}
	b, _ := r.Marshal()
	return raftpb.Entry{Index: index, Term: term, Type: raftpb.EntryNormal, Data: b}
}

func start(t *testing.T, walDir, snapshot, out string) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- record.Run(ctx, record.Config{WALDir: walDir, OutputDir: out, SnapshotSource: snapshot, PollInterval: 2 * time.Millisecond, Ready: func(capture.Manifest) { close(ready) }})
	}()
	select {
	case <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("recorder failed before ready: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("recorder readiness timeout; may be blocked on the live WAL lock")
	}
	return cancel, done
}

func stop(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("recorder shutdown timeout")
	}
}

func TestLockedLiveWALCanonicalReplayInputAndSourcePreservation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "source-wal")
	w := newWAL(t, walDir)
	defer w.Close()
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	out := filepath.Join(dir, "capture")
	cancel, done := start(t, walDir, snapshot, out)
	// The writer remains open and holds the native WAL lock throughout capture.
	save(t, w, 1, 1, entry(2, 1, "old2"), entry(3, 1, "old3"), entry(4, 1, "old4"))
	save(t, w, 2, 3, entry(2, 2, "new2"), entry(3, 2, "new3"))
	save(t, w, 2, 3, entry(4, 2, "uncommitted"))
	name := "0000000000000000-0000000000000000.wal"
	before, err := capture.FileSHA256(filepath.Join(walDir, name))
	if err != nil {
		t.Fatal(err)
	}
	stop(t, cancel, done)
	after, _ := capture.FileSHA256(filepath.Join(walDir, name))
	if before != after {
		t.Fatal("recorder modified the source WAL")
	}
	d, err := capture.Load(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Entries) != 2 || d.Entries[0].Index != 2 || d.Entries[0].Term != 2 || d.Entries[1].Index != 3 {
		t.Fatalf("unexpected canonical entries: %+v", d.Entries)
	}
	if d.HardState.Commit != 3 || d.Manifest.MemberID != 11 || d.Manifest.ClusterID != 22 {
		t.Fatalf("bad state or identity: %+v", d.Manifest)
	}
	for _, e := range d.Entries {
		if _, ok := d.EntryTimes[e.Index]; !ok {
			t.Fatalf("missing time anchor for %d", e.Index)
		}
	}
	prepared, err := capture.Prepare(context.Background(), out, filepath.Join(t.TempDir(), "index"))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Count != 2 || prepared.IndexBytes != 2*capture.EntryIndexBytes {
		t.Fatalf("unexpected prepared index: %+v", prepared)
	}
	stream, err := prepared.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range d.Entries {
		got, at, err := stream.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got.Index != want.Index || got.Term != want.Term || !bytes.Equal(got.Data, want.Data) || at != d.EntryTimes[want.Index] {
			t.Fatalf("stream disagrees with canonical loader at %d", want.Index)
		}
	}
	if _, _, err = stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("expected stream EOF, got %v", err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	anchorPath := filepath.Join(out, "anchors.jsonl")
	anchorBytes, err := os.ReadFile(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(anchorPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = capture.Load(out); err == nil || !strings.Contains(err.Error(), "no time anchor") {
		t.Fatalf("expected missing anchor rejection, got %v", err)
	}
	if _, err = capture.Prepare(context.Background(), out, filepath.Join(t.TempDir(), "index")); err == nil || !strings.Contains(err.Error(), "no time anchor") {
		t.Fatalf("expected streaming missing anchor rejection, got %v", err)
	}
	if err = os.WriteFile(anchorPath, anchorBytes, 0600); err != nil {
		t.Fatal(err)
	}
	// Capture corruption must fail before the analyzer applies any entry.
	p := filepath.Join(out, "wal", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 1
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Load(out); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum rejection, got %v", err)
	}
	b[len(b)-1] ^= 1
	pos := bytes.Index(b, []byte("new2"))
	if pos < 0 {
		t.Fatal("fixture payload not found")
	}
	b[pos] ^= 1
	if err = os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	m := d.Manifest
	m.WALFiles[0].SHA256, err = capture.FileSHA256(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = capture.WriteManifest(out, m); err != nil {
		t.Fatal(err)
	}
	if _, err = capture.Load(out); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected native WAL CRC rejection, got %v", err)
	}
	if _, err = capture.Prepare(context.Background(), out, filepath.Join(t.TempDir(), "index")); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected streaming native CRC rejection before stream opens, got %v", err)
	}
}

func TestStreamingPayloadMemoryIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("48 MiB streaming fixture")
	}
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w := newWAL(t, walDir)
	defer w.Close()
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	const entries = 768
	payload := strings.Repeat("x", 64<<10)
	for i := uint64(2); i <= entries+1; i++ {
		save(t, w, 1, i, entry(i, 1, payload))
	}
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	out := filepath.Join(dir, "capture")
	cancel, done := start(t, walDir, snapshot, out)
	stop(t, cancel, done)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	p, err := capture.Prepare(context.Background(), out, filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if p.Count != entries {
		t.Fatalf("prepared count %d", p.Count)
	}
	if after.HeapAlloc > before.HeapAlloc+(12<<20) {
		t.Fatalf("Prepare retained payload-sized memory: before=%d after=%d", before.HeapAlloc, after.HeapAlloc)
	}
	stream, err := p.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var total int64
	for i := 0; i < entries; i++ {
		e, _, err := stream.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		total += int64(len(e.Data))
		if i%128 == 0 {
			runtime.GC()
			runtime.ReadMemStats(&after)
			if after.HeapAlloc > before.HeapAlloc+(12<<20) {
				t.Fatalf("stream retained prior payloads at entry %d: %d bytes", i, after.HeapAlloc-before.HeapAlloc)
			}
		}
	}
	if total < entries*(64<<10) {
		t.Fatal("fixture did not exercise the expected payload volume")
	}
	if _, _, err = stream.Next(context.Background()); err != io.EOF {
		t.Fatal(err)
	}
	t.Logf("streamed payload=%d bytes; entry index=%d bytes; anchors=%d bytes", total, p.IndexBytes, p.AnchorBytes)
}

func TestStreamingSuffixReplacementAcrossWALRotations(t *testing.T) {
	oldSize := wal.SegmentSizeBytes
	wal.SegmentSizeBytes = 1024
	defer func() { wal.SegmentSizeBytes = oldSize }()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w := newWAL(t, walDir)
	defer w.Close()
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	out := filepath.Join(dir, "capture")
	cancel, done := start(t, walDir, snapshot, out)
	large := strings.Repeat("a", 2048)
	save(t, w, 1, 1, entry(2, 1, "old2"+large), entry(3, 1, "old3"+large), entry(4, 1, "old4"+large))
	save(t, w, 1, 1)
	save(t, w, 2, 3, entry(2, 2, "new2"+large), entry(3, 2, "new3"+large))
	save(t, w, 2, 3)
	save(t, w, 2, 3, entry(4, 2, "uncommitted"))
	stop(t, cancel, done)
	p, err := capture.Prepare(context.Background(), out, filepath.Join(dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Manifest.WALFiles) < 3 {
		t.Fatal("fixture did not rotate WAL")
	}
	if p.Count != 2 || p.IndexBytes != 2*capture.EntryIndexBytes {
		t.Fatalf("canonical disk index did not truncate suffix: count=%d bytes=%d", p.Count, p.IndexBytes)
	}
	s, err := p.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for index := uint64(2); index <= 3; index++ {
		e, _, err := s.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if e.Index != index || e.Term != 2 || !bytes.Contains(e.Data, []byte(fmt.Sprintf("new%d", index))) {
			t.Fatalf("wrong canonical entry: index=%d term=%d", e.Index, e.Term)
		}
	}
	if _, _, err = s.Next(context.Background()); err != io.EOF {
		t.Fatal(err)
	}
}

func TestRecorderExcludesTornActiveTail(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w := newWAL(t, walDir)
	defer w.Close()
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	snapshot := filepath.Join(dir, "baseline-source.db")
	baseline(t, snapshot, 1)
	name := "0000000000000000-0000000000000000.wal"
	f, err := os.Open(filepath.Join(walDir, name))
	if err != nil {
		t.Fatal(err)
	}
	d := wal.NewDecoder(fileutil.NewFileReader(f))
	for {
		var rec walpb.Record
		if err := d.Decode(&rec); err != nil {
			break
		}
		if rec.Type == wal.CrcType {
			d.UpdateCRC(rec.Crc)
		}
	}
	offset := d.LastOffset()
	f.Close()
	f, err = os.OpenFile(filepath.Join(walDir, name), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var partial [11]byte
	binary.LittleEndian.PutUint64(partial[:8], 100)
	copy(partial[8:], []byte{1, 2, 3})
	if _, err = f.WriteAt(partial[:], offset); err != nil {
		t.Fatal(err)
	}
	f.Close()
	out := filepath.Join(dir, "capture")
	cancel, done := start(t, walDir, snapshot, out)
	stop(t, cancel, done)
	captured, err := capture.Load(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.Manifest.Warnings) == 0 {
		t.Fatal("excluded torn tail was not reported")
	}
	if captured.Manifest.WALFiles[0].Size != offset {
		t.Fatalf("captured torn bytes: got %d want %d", captured.Manifest.WALFiles[0].Size, offset)
	}
}

func TestRecorderRejectsLiveBackendAndDoesNotCreateOutput(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "member", "wal")
	if err := os.MkdirAll(filepath.Join(dir, "member", "snap"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(walDir, 0700); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(dir, "member", "snap", "db")
	baseline(t, live, 1)
	out := filepath.Join(dir, "capture")
	err := record.Run(context.Background(), record.Config{WALDir: walDir, OutputDir: out, SnapshotSource: live})
	if err == nil || !strings.Contains(err.Error(), "live") {
		t.Fatalf("expected live backend rejection, got %v", err)
	}
	if _, err = os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("recorder created output despite invalid source: %v", err)
	}
}

func TestRecorderDetectsMissingWALSegment(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w := newWAL(t, walDir)
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(walDir, "0000000000000000-0000000000000000.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(walDir, "0000000000000002-0000000000000002.wal"), b, 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	err = record.Run(context.Background(), record.Config{WALDir: walDir, OutputDir: filepath.Join(dir, "capture"), SnapshotSource: snapshot})
	if err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("expected missing segment rejection, got %v", err)
	}
}

func TestOutputSymlinkCannotTargetSourceWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.Mkdir(walDir, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(walDir, alias); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	err := record.Run(context.Background(), record.Config{WALDir: walDir, OutputDir: filepath.Join(alias, "capture"), SnapshotSource: snapshot})
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("expected source-path rejection, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(walDir, "capture")); !os.IsNotExist(err) {
		t.Fatalf("capture created inside source WAL: %v", err)
	}
}

func TestSnapshotCommandProvider(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w := newWAL(t, walDir)
	defer w.Close()
	save(t, w, 1, 1, entry(1, 1, "baseline"))
	snapshot := filepath.Join(dir, "snapshot.db")
	baseline(t, snapshot, 1)
	t.Setenv("ETCD_TREND_SNAPSHOT_HELPER", "1")
	t.Setenv("ETCD_TREND_SNAPSHOT_SOURCE", snapshot)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := filepath.Join(dir, "capture")
	err := record.Run(ctx, record.Config{WALDir: walDir, OutputDir: out, SnapshotCommand: []string{os.Args[0], "-test.run=^TestSnapshotCommandHelper$"}, Ready: func(capture.Manifest) { cancel() }})
	if err != nil {
		t.Fatal(err)
	}
	d, err := capture.Load(out)
	if err != nil {
		t.Fatal(err)
	}
	if d.Manifest.Snapshot.Provider != "command" {
		t.Fatal("wrong provider recorded")
	}
}

func TestSnapshotCommandHelper(t *testing.T) {
	if os.Getenv("ETCD_TREND_SNAPSHOT_HELPER") != "1" {
		return
	}
	b, err := os.ReadFile(os.Getenv("ETCD_TREND_SNAPSHOT_SOURCE"))
	if err != nil {
		os.Exit(2)
	}
	if err = os.WriteFile(os.Getenv("CAPTURE_SNAPSHOT_PATH"), b, 0600); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}
