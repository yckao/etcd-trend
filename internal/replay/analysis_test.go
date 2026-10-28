package replay

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/third_party/etcd-server/lease"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/backend"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/mvcc"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestActualObjectAnalysisPreservesReplay(t *testing.T) {
	path := baseline(t)
	entries := workload()
	keyNames := map[string]string{"a": "/registry/configmaps/default/hot", "b": "/registry/pods/default/b", "c": "/registry/pods/default/c", "leased": "/registry/configmaps/default/leased", "wrong": "/registry/configmaps/default/untaken"}
	for i := range entries {
		var req pb.InternalRaftRequest
		if err := req.Unmarshal(entries[i].Data); err != nil {
			t.Fatal(err)
		}
		if req.Put != nil {
			req.Put.Key = []byte(keyNames[string(req.Put.Key)])
		}
		if req.Txn != nil {
			for _, c := range req.Txn.Compare {
				c.Key = []byte(keyNames[string(c.Key)])
			}
			for _, ops := range [][]*pb.RequestOp{req.Txn.Success, req.Txn.Failure} {
				for _, op := range ops {
					p := op.GetRequestPut()
					p.Key = []byte(keyNames[string(p.Key)])
				}
			}
		}
		entries[i] = operation(entries[i].Index, &req)
	}
	prefix := "/registry/pods/default/"
	entries = append(entries,
		operation(9, &pb.InternalRaftRequest{DeleteRange: &pb.DeleteRangeRequest{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix))}}),
		operation(10, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 8, Physical: true}}),
		operation(11, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte(keyNames["a"]), Value: []byte(strings.Repeat("h", 32768))}}),
		operation(12, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("/registry/secrets/default/stable"), Value: []byte(strings.Repeat("s", 65536))}}),
	)
	times := map[uint64]time.Duration{}
	for _, e := range entries {
		times[e.Index] = time.Duration(e.Index) * 50 * time.Millisecond
	}
	config := Config{BaselinePath: path, Entries: entries, EntryTimes: times, CompactionBatchLimit: 1, CompactionSleepInterval: time.Millisecond, OutputDir: filepath.Join(t.TempDir(), "plain")}
	plain, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	var first *analysis.Summary
	for _, speed := range []float64{0, 100, 1} {
		config.OutputDir = filepath.Join(t.TempDir(), "analyzed")
		config.AnalyzeObjects, config.AnalysisWindow, config.AnalysisTopN, config.Speed = true, 200*time.Millisecond, 10, speed
		result, err := Run(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}
		if result.LogicalSHA256 != plain.LogicalSHA256 || result.CommitsSHA256 != plain.CommitsSHA256 || result.OperationsSHA256 != plain.OperationsSHA256 {
			t.Fatal("object observation changed replay logical state or physical commit curve")
		}
		s := result.Analysis
		if s == nil || s.Metrics.Puts != 8 || s.Metrics.Deletes != 3 || s.Metrics.ValueWriteBytes != 98328 || s.Final.LiveObjects != 2 || s.Final.LiveValueBytes != 98304 || s.Metrics.CompactedRecords == 0 {
			t.Fatalf("incorrect actual mutation accounting: %+v", s)
		}
		if s.Metrics.EstimatedDBGrowthBytes+s.UnattributedDBGrowthBytes != s.DBGrowthBytes || s.Metrics.EstimatedFreeGrowthBytes+s.UnattributedFreeGrowthBytes != s.FreeGrowthBytes || s.Metrics.EstimatedFreeReuseBytes+s.UnattributedFreeReuseBytes != s.FreeReuseBytes {
			t.Fatal("physical attribution does not reconcile")
		}
		var retained, records int64
		if err := mvcc.ReplayVisitRecords(context.Background(), filepath.Join(config.OutputDir, "replay.db"), func(e mvcc.ReplayMutation) error { retained += e.RecordBytes; records++; return nil }); err != nil {
			t.Fatal(err)
		}
		if retained != s.Final.RetainedBytes || records != s.Final.RetainedRecords {
			t.Fatal("analysis final stock differs from actual replay DB")
		}
		f, err := os.Open(filepath.Join(config.OutputDir, "analysis", "objects.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(f)
		var puts, deletes int64
		for {
			var row analysis.Row
			if err := dec.Decode(&row); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(row.Identity.Key, "untaken") {
				t.Fatal("untaken transaction branch attributed")
			}
			puts += row.Metrics.Puts
			deletes += row.Metrics.Deletes
		}
		f.Close()
		if puts != 8 || deletes != 3 {
			t.Fatal("object window totals differ from actual mutations")
		}
		if first == nil {
			first = s
		} else {
			for file, sum := range first.SHA256 {
				if s.SHA256[file] != sum {
					t.Fatalf("analysis %s changed at speed %g", file, speed)
				}
			}
		}
	}
}

func TestObjectAnalysisIncludesBaselineHistory(t *testing.T) {
	path := baseline(t)
	b := backend.NewDefaultBackend(zap.NewNop(), path)
	kv := mvcc.NewStore(zap.NewNop(), b, &lease.FakeLessor{}, mvcc.StoreConfig{})
	kv.Put([]byte("/registry/configmaps/default/hot"), []byte("old"), 0)
	kv.Put([]byte("/registry/configmaps/default/hot"), []byte("current"), 0)
	kv.Put([]byte("/registry/secrets/default/stable"), []byte(strings.Repeat("s", 65536)), 0)
	kv.Close()
	b.Close()
	before, err := capture.FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Run(context.Background(), Config{BaselinePath: path, OutputDir: filepath.Join(t.TempDir(), "analyzed"), AnalyzeObjects: true})
	if err != nil {
		t.Fatal(err)
	}
	after, err := capture.FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	s := r.Analysis
	if before != after || s.Initial != s.Final || s.Initial.RetainedRecords != 3 || s.Initial.LiveObjects != 2 || s.Initial.HistoryBytes <= 3 || s.Metrics.Puts != 0 || s.Metrics.ValueWriteBytes != 0 {
		t.Fatalf("baseline state missing, mutated, or counted as workload: %+v", s)
	}
}

// Native bbolt publishes free-page statistics one commit after compaction moves
// pages into the pending list. Accounting must follow that release to its cause.
func TestCompactionPendingPagesRemainAttributable(t *testing.T) {
	path := baseline(t)
	key := []byte("/registry/configmaps/default/hot")
	config := Config{BaselinePath: path, OutputDir: filepath.Join(t.TempDir(), "analyzed"), AnalyzeObjects: true, AnalysisWindow: 10 * time.Millisecond, CompactionBatchLimit: 4, CompactionSleepInterval: time.Millisecond, EntryTimes: make(map[uint64]time.Duration)}
	for i := uint64(1); i <= 32; i++ {
		config.Entries = append(config.Entries, operation(i, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: key, Value: []byte(strings.Repeat("v", 32768))}}))
		config.EntryTimes[i] = time.Duration(i) * time.Millisecond
	}
	config.Entries = append(config.Entries, operation(33, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 33, Physical: true}}))
	config.EntryTimes[33] = 33 * time.Millisecond
	r, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	s := r.Analysis
	if s.Metrics.CompactedRecords != 31 || s.FreeGrowthBytes == 0 || s.Metrics.EstimatedFreeGrowthBytes < s.FreeGrowthBytes/2 {
		t.Fatalf("native compaction free-page release lost attribution: %+v", s)
	}
	if s.Final.RetainedRecords != 1 || s.Final.HistoryBytes != 0 {
		t.Fatalf("compacted stock incorrect: %+v", s.Final)
	}
}
