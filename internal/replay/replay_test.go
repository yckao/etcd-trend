package replay

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func baseline(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "baseline.db")
	b := backend.NewDefaultBackend(zap.NewNop(), p)
	kv := mvcc.NewStore(zap.NewNop(), b, nil, mvcc.StoreConfig{})
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}
func operation(i uint64, r *pb.InternalRaftRequest) raftpb.Entry {
	data, err := r.Marshal()
	if err != nil {
		panic(err)
	}
	return raftpb.Entry{Index: i, Term: 1, Type: raftpb.EntryNormal, Data: data}
}
func put(k, v string) *pb.RequestOp {
	return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(k), Value: []byte(v)}}}
}
func workload() []raftpb.Entry {
	return []raftpb.Entry{
		operation(1, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("v1")}}),
		operation(2, &pb.InternalRaftRequest{Txn: &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 2}}}, Success: []*pb.RequestOp{put("a", "v2"), put("b", "z")}, Failure: []*pb.RequestOp{put("wrong", "bad")}}}),
		operation(3, &pb.InternalRaftRequest{Txn: &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 2}}}, Success: []*pb.RequestOp{put("wrong", "bad")}, Failure: []*pb.RequestOp{put("c", "fallback")}}}),
		operation(4, &pb.InternalRaftRequest{LeaseGrant: &pb.LeaseGrantRequest{ID: 999, TTL: 1}}),
		operation(5, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("leased"), Value: []byte("temporary"), Lease: 999}}),
		operation(6, &pb.InternalRaftRequest{LeaseRevoke: &pb.LeaseRevokeRequest{ID: 999}}),
		operation(7, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 4, Physical: true}}),
		operation(8, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("v3")}}),
	}
}
func TestSpeedsPreserveStateAndCommitSchedule(t *testing.T) {
	path := baseline(t)
	entries := workload()
	times := map[uint64]time.Duration{}
	for _, e := range entries {
		times[e.Index] = time.Duration(e.Index) * 50 * time.Millisecond
	}
	var reference Result
	for _, speed := range []float64{0, 100, 1} {
		r, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: path, Entries: entries, EntryTimes: times, OutputDir: filepath.Join(t.TempDir(), "replay"), Speed: speed, CompactionBatchLimit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if r.FinalRevision != 7 || r.KeyCount != 3 || r.FinalIndex != 8 {
			t.Fatalf("unexpected state: %+v", r)
		}
		for _, op := range r.Operations {
			if op.Error != "" {
				t.Fatalf("unexpected operation error: %+v", op)
			}
		}
		want := map[string]string{"a": "v3", "b": "z", "c": "fallback"}
		for _, kv := range r.KVs {
			if want[string(kv.Key)] != string(kv.Value) {
				t.Fatalf("unexpected KV: %+v", kv)
			}
		}
		if len(r.Commits) == 0 {
			t.Fatal("no commits observed")
		}
		if speed == 0 {
			reference = r
			continue
		}
		if r.LogicalSHA256 != reference.LogicalSHA256 {
			t.Fatalf("digest differs at speed %g", speed)
		}
		if !reflect.DeepEqual(r.Commits, reference.Commits) {
			t.Fatalf("commits differ at speed %g\n got %+v\nwant %+v", speed, r.Commits, reference.Commits)
		}
		if !reflect.DeepEqual(r.Operations, reference.Operations) {
			t.Fatalf("operations differ at speed %g", speed)
		}
	}
}
func TestInputGuards(t *testing.T) {
	path := baseline(t)
	t.Run("existing output", func(t *testing.T) {
		_, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: path, OutputDir: t.TempDir()})
		if err == nil {
			t.Fatal("expected existing directory error")
		}
	})
	t.Run("entry gap", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "out")
		_, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: path, OutputDir: out, Entries: []raftpb.Entry{{Index: 2}}, EntryTimes: map[uint64]time.Duration{2: time.Second}})
		if err == nil {
			t.Fatal("expected gap error")
		}
		if _, err = os.Stat(out); !os.IsNotExist(err) {
			t.Fatal("invalid input created output")
		}
	})
	t.Run("missing anchor", func(t *testing.T) {
		_, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: path, OutputDir: filepath.Join(t.TempDir(), "out"), Entries: []raftpb.Entry{{Index: 1}}})
		if err == nil {
			t.Fatal("expected time error")
		}
	})
}

func TestRestartMembershipRequestsUseMembershipApplier(t *testing.T) {
	path := baseline(t)
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("members"))
		if err != nil {
			return err
		}
		member, err := json.Marshal(&membership.Member{ID: 1})
		if err != nil {
			return err
		}
		return b.Put(schema.BackendMemberKey(1), member)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	entries := []raftpb.Entry{
		operation(1, &pb.InternalRaftRequest{ClusterVersionSet: &membershippb.ClusterVersionSetRequest{Ver: "3.6.0"}}),
		operation(2, &pb.InternalRaftRequest{ClusterMemberAttrSet: &membershippb.ClusterMemberAttrSetRequest{Member_ID: 1, MemberAttributes: &membershippb.Attributes{Name: "restarted", ClientUrls: []string{"http://localhost:2379"}}}}),
		operation(3, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("ok")}}),
	}
	r, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: path, OutputDir: filepath.Join(t.TempDir(), "out"), Entries: entries, EntryTimes: map[uint64]time.Duration{1: 0, 2: time.Millisecond, 3: 2 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if r.KeyCount != 1 || r.FinalRevision != 2 {
		t.Fatalf("unexpected state: %+v", r)
	}
	if r.Operations[1].Type != "member-attributes" {
		t.Fatal("membership request was not replayed")
	}
}

func TestNoopSuffixPersistsFinalIndex(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	r, err := Run(context.Background(), Config{RetainObservations: true, BaselinePath: baseline(t), OutputDir: out,
		Entries:    []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryNormal}},
		EntryTimes: map[uint64]time.Duration{1: 0}})
	if err != nil {
		t.Fatal(err)
	}
	info, err := etcdutil.InspectDB(filepath.Join(out, "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Index != r.FinalIndex || info.Term != 1 {
		t.Fatalf("persisted progress %+v differs from report %+v", info, r)
	}
	if len(r.Commits) != 1 || r.Commits[0].Reason != "finalize" {
		t.Fatalf("unexpected final boundary %+v", r.Commits)
	}
}

func TestCancelDuringVirtualCompactionSleep(t *testing.T) {
	path := baseline(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entries := []raftpb.Entry{
		operation(1, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("first")}}),
		operation(2, &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("second")}}),
		operation(3, &pb.InternalRaftRequest{Compaction: &pb.CompactionRequest{Revision: 2, Physical: true}}),
	}
	r, err := Run(ctx, Config{RetainObservations: true, BaselinePath: path, OutputDir: filepath.Join(t.TempDir(), "out"),
		Entries: entries, EntryTimes: map[uint64]time.Duration{1: 0, 2: 0, 3: 0}, Speed: 1,
		CompactionBatchLimit: 1, CompactionSleepInterval: time.Second,
		OnCommit: func(c Commit) {
			if c.Index == 3 {
				cancel()
			}
		}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if r.AppliedEntries != 2 {
		t.Fatalf("cancellation did not occur during compaction: applied=%d", r.AppliedEntries)
	}
}
