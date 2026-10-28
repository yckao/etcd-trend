package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/third_party/etcd-server/lease"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/backend"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/mvcc"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestMetadataFromActualBaselineAndSelectedMVCCMutations(t *testing.T) {
	path := baseline(t)
	protobuf, err := os.ReadFile("../analysis/testdata/upstream-pod.pb")
	if err != nil {
		t.Fatal(err)
	}
	jsonValue, err := os.ReadFile("../analysis/testdata/upstream-pod.json")
	if err != nil {
		t.Fatal(err)
	}
	key := "/registry/pods/tenant-blue/worker-0"
	b := backend.NewDefaultBackend(zap.NewNop(), path)
	kv := mvcc.NewStore(zap.NewNop(), b, &lease.FakeLessor{}, mvcc.StoreConfig{})
	kv.Put([]byte(key), protobuf, 0)
	kv.Close()
	b.Close()
	green := bytes.ReplaceAll(jsonValue, []byte(`"tenant":"blue"`), []byte(`"tenant":"green"`))
	red := bytes.ReplaceAll(jsonValue, []byte(`"tenant":"blue"`), []byte(`"tenant":"red"`))
	recreated := bytes.ReplaceAll(red, []byte("pod-uid-001"), []byte("pod-uid-002"))
	entries := []raftpb.Entry{
		operation(1, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte(key), Value: green}}),
		operation(2, &pb.InternalRaftRequest{Txn: &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte(key), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 3}}}, Success: []*pb.RequestOp{put(key, string(red))}, Failure: []*pb.RequestOp{put(key, "untaken opaque value")}}}),
		operation(3, &pb.InternalRaftRequest{DeleteRange: &pb.DeleteRangeRequest{Key: []byte(key)}}),
		operation(4, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 5, Physical: true}}),
		operation(5, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte(key), Value: recreated}}),
	}
	config := Config{BaselinePath: path, Entries: entries, EntryTimes: map[uint64]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 1100 * time.Millisecond, 4: 1200 * time.Millisecond, 5: 2100 * time.Millisecond}, OutputDir: filepath.Join(t.TempDir(), "plain")}
	plain, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	config.OutputDir, config.AnalyzeObjects, config.AnalysisWindow, config.AnalysisLabelKeys = filepath.Join(t.TempDir(), "metadata"), true, time.Second, []string{"tenant"}
	result, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.LogicalSHA256 != plain.LogicalSHA256 || result.OperationsSHA256 != plain.OperationsSHA256 || result.Analysis.Metrics.Puts != 3 || result.Analysis.Metrics.Deletes != 1 {
		t.Fatal("metadata extraction changed actual replay work")
	}
	read := func(name string, out any) {
		t.Helper()
		f, err := os.Open(filepath.Join(config.OutputDir, "analysis", name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := json.NewDecoder(f).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	var baseline, final struct{ Metadata analysis.Metadata }
	read("baseline.objects.jsonl", &baseline)
	read("final.objects.jsonl", &final)
	if baseline.Metadata.Status != "decoded-protobuf" || baseline.Metadata.Labels["tenant"] != "blue" || final.Metadata.Status != "decoded-json" || final.Metadata.Labels["tenant"] != "red" || final.Metadata.UID != "pod-uid-002" {
		t.Fatalf("missing actual value observations: baseline=%+v final=%+v", baseline, final)
	}
	var row analysis.Row
	read("objects.jsonl", &row)
	if row.Metadata.Labels["tenant"] != "red" || row.Metadata.IdentityMismatch || row.Metrics.Puts != 2 {
		t.Fatalf("wrong selected branch/window endpoint metadata: %+v", row)
	}
}
