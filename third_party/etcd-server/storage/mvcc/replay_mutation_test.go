// Copyright 2026 etcd-trend contributors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yckao/etcd-trend/third_party/etcd-server/lease"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/backend"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.uber.org/zap"
)

func copyObservedEvent(e ReplayMutation) ReplayMutation {
	e.Key = append([]byte(nil), e.Key...)
	e.Value = append([]byte(nil), e.Value...)
	return e
}

func saveReplayImage(t *testing.T, b backend.Backend, path string) {
	t.Helper()
	s := b.Snapshot()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.WriteTo(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func visitReplayImage(t *testing.T, path string) []ReplayMutation {
	t.Helper()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var events []ReplayMutation
	err = ReplayVisitRecords(context.Background(), path, func(e ReplayMutation) error {
		events = append(events, copyObservedEvent(e))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("baseline visitor changed the image")
	}
	return events
}

func TestReplayMutationConservationAndObservationInvariance(t *testing.T) {
	type observedRun struct {
		events        []ReplayMutation
		before, after []ReplayMutation
		commits       []backend.ReplayCommitStats
	}
	run := func(observe bool) observedRun {
		var result observedRun
		dir := t.TempDir()
		bcfg := backend.DefaultBackendConfig(zap.NewNop())
		bcfg.Path = filepath.Join(dir, "db")
		bcfg.ReplayManualCommit = true
		bcfg.BatchLimit = 5
		bcfg.OnReplayCommit = func(s backend.ReplayCommitStats) { result.commits = append(result.commits, s) }
		b := backend.New(bcfg)
		cfg := StoreConfig{ReplaySynchronousCompaction: true, CompactionBatchLimit: 1}
		if observe {
			cfg.OnReplayMutation = func(e ReplayMutation) { result.events = append(result.events, copyObservedEvent(e)) }
		}
		s := NewStore(zap.NewNop(), b, &lease.FakeLessor{}, cfg)
		s.Put([]byte("a"), []byte("a1"), lease.NoLease)
		s.Put([]byte("b"), []byte("b1"), lease.NoLease)
		tx := s.Write(traceutil.TODO())
		tx.Put([]byte("a"), []byte("a2"), lease.NoLease)
		tx.Put([]byte("c"), []byte("c1"), lease.NoLease)
		tx.End()
		s.DeleteRange([]byte("b"), []byte("c"))
		s.DeleteRange([]byte("missing"), nil)
		s.DeleteRange([]byte("a"), []byte("d"))
		s.Put([]byte("a"), []byte("a3"), lease.NoLease)
		if s.Rev() != 7 {
			t.Fatalf("unexpected workload revision %d", s.Rev())
		}
		beforePath := filepath.Join(dir, "before.db")
		saveReplayImage(t, b, beforePath)
		result.before = visitReplayImage(t, beforePath)
		done, err := s.Compact(traceutil.TODO(), 7)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		afterPath := filepath.Join(dir, "after.db")
		saveReplayImage(t, b, afterPath)
		result.after = visitReplayImage(t, afterPath)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	observed, plain := run(true), run(false)
	if !reflect.DeepEqual(observed.commits, plain.commits) {
		t.Fatalf("observer changed physical commit sequence:\nobserved=%+v\nplain=%+v", observed.commits, plain.commits)
	}
	if !reflect.DeepEqual(observed.before, plain.before) || !reflect.DeepEqual(observed.after, plain.after) {
		t.Fatal("observer changed retained logical records")
	}
	if len(observed.before) != 8 || len(observed.after) != 1 {
		t.Fatalf("wrong retained counts before=%d after=%d", len(observed.before), len(observed.after))
	}
	if len(observed.events) != 15 {
		t.Fatalf("want8 writes+7 removals, got%d", len(observed.events))
	}
	remaining := make(map[string]ReplayMutation)
	for _, e := range observed.events {
		identity := fmt.Sprintf("%d.%d", e.Revision, e.SubRevision)
		if e.Kind == "compact" {
			old, ok := remaining[identity]
			if !ok {
				t.Fatalf("removed unknown record %s", identity)
			}
			old.Kind = e.Kind
			if !reflect.DeepEqual(old, e) {
				t.Fatalf("removal differs from written record: %+v vs %+v", old, e)
			}
			delete(remaining, identity)
		} else {
			if e.Kind != "put" && e.Kind != "delete" {
				t.Fatalf("unexpected mutation %q", e.Kind)
			}
			if e.Tombstone != (e.Kind == "delete") {
				t.Fatalf("wrong tombstone classification %+v", e)
			}
			if e.Kind == "put" && e.ValueBytes != 2 {
				t.Fatalf("wrong value bytes %+v", e)
			}
			if e.Kind == "delete" && (e.ValueBytes != 0 || e.RecordBytes != 21) {
				t.Fatalf("wrong tombstone bytes %+v", e)
			}
			remaining[identity] = e
		}
	}
	if len(remaining) != 1 {
		t.Fatalf("wrong conservation result %+v", remaining)
	}
	for _, actual := range observed.after {
		expected := remaining[fmt.Sprintf("%d.%d", actual.Revision, actual.SubRevision)]
		expected.Kind = "baseline"
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("baseline scan differs from remaining mutations: %+v vs %+v", expected, actual)
		}
	}
	for i, actual := range observed.before {
		expected := observed.events[i]
		expected.Kind = "baseline"
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("baseline scan differs at record%d: %+v vs %+v", i, expected, actual)
		}
	}
}

func TestReplayMutationPrecedesCommit(t *testing.T) {
	var order []string
	bcfg := backend.DefaultBackendConfig(zap.NewNop())
	bcfg.Path = filepath.Join(t.TempDir(), "db")
	bcfg.ReplayManualCommit = true
	bcfg.BatchLimit = 1
	bcfg.OnReplayCommit = func(backend.ReplayCommitStats) { order = append(order, "commit") }
	b := backend.New(bcfg)
	s := NewStore(zap.NewNop(), b, &lease.FakeLessor{}, StoreConfig{OnReplayMutation: func(e ReplayMutation) { order = append(order, e.Kind) }})
	order = nil
	s.Put([]byte("a"), []byte("value"), lease.NoLease)
	if !reflect.DeepEqual(order, []string{"put", "commit"}) {
		t.Fatalf("wrong observation ordering: %v", order)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayRecordVisitorCancellationAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ReplayVisitRecords(ctx, "does-not-exist", func(ReplayMutation) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("got%v", err)
	}
	for _, key := range [][]byte{nil, []byte("short"), make([]byte, 17), append(RevToBytes(Revision{Main: 1}, NewRevBytes()), 'x')} {
		if _, err := replayMutationFromRecord("baseline", key, nil); err == nil {
			t.Fatalf("accepted malformed revision %x", key)
		}
	}
}
