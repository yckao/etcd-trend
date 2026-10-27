package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

type generatedSource struct{ position, total int }

func (s *generatedSource) Next(ctx context.Context) (raftpb.Entry, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return raftpb.Entry{}, 0, err
	}
	if s.position == s.total {
		return raftpb.Entry{}, 0, io.EOF
	}
	s.position++
	r := &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte(fmt.Sprintf("key/%04d", s.position%97)), Value: []byte(fmt.Sprintf("value/%06d", s.position))}}
	return operation(uint64(s.position), r), time.Duration(s.position) * time.Millisecond, nil
}

func TestStreamingReportsHaveBoundedRetention(t *testing.T) {
	path := baseline(t)
	var first Result
	for _, speed := range []float64{0, 100} {
		out := filepath.Join(t.TempDir(), "run")
		r, err := Run(context.Background(), Config{BaselinePath: path, SourceVersion: "3.5.34", Source: &generatedSource{total: 1000}, OutputDir: out, Speed: speed})
		if err != nil {
			t.Fatal(err)
		}
		if r.AppliedEntries != 1000 || r.OperationCount != 1000 || r.KeyCount != 97 || r.CommitCount == 0 {
			t.Fatalf("wrong counts: %+v", r)
		}
		if len(r.Commits) != 0 || len(r.Operations) != 0 || len(r.KVs) != 0 {
			t.Fatal("streaming mode retained observations")
		}
		b, err := os.ReadFile(filepath.Join(out, "report.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 8192 || strings.Contains(string(b), `"commits":`) || strings.Contains(string(b), `"operations":`) {
			t.Fatal("report contains unbounded observations")
		}
		for _, sidecar := range []struct {
			name, hash string
			count      int
		}{{r.CommitsFile, r.CommitsSHA256, r.CommitCount}, {r.OperationsFile, r.OperationsSHA256, r.OperationCount}} {
			file, err := os.Open(filepath.Join(out, sidecar.name))
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			_, err = io.Copy(h, file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(h.Sum(nil)) != sidecar.hash {
				t.Fatal("sidecar digest mismatch")
			}
			file, err = os.Open(filepath.Join(out, sidecar.name))
			if err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(file)
			n := 0
			for {
				var rec map[string]any
				err = dec.Decode(&rec)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
			file.Close()
			if n != sidecar.count {
				t.Fatalf("sidecar count=%d want%d", n, sidecar.count)
			}
		}
		if speed == 0 {
			first = r
		} else if r.LogicalSHA256 != first.LogicalSHA256 || r.CommitsSHA256 != first.CommitsSHA256 || r.OperationsSHA256 != first.OperationsSHA256 {
			t.Fatal("streaming speed changed replay results")
		}
	}
}

func TestStreamingRejectsUnorderedInput(t *testing.T) {
	path := baseline(t)
	out := filepath.Join(t.TempDir(), "out")
	source := &sliceSource{entries: []raftpb.Entry{{Index: 1, Term: 1}, {Index: 3, Term: 1}}, times: map[uint64]time.Duration{1: 0, 3: 1}}
	_, err := Run(context.Background(), Config{BaselinePath: path, Source: source, OutputDir: out})
	if err == nil || !strings.Contains(err.Error(), "entry gap") {
		t.Fatalf("want gap error, got%v", err)
	}
	if _, err = os.Stat(filepath.Join(out, "report.json")); !os.IsNotExist(err) {
		t.Fatal("failed replay published successful report")
	}
}

func TestLegacyMetadataAdapters(t *testing.T) {
	for _, legacy := range []*pb.Request{
		{ID: 1, Method: "PUT", Path: "/0/version", Val: "3.4.0"},
		{ID: 2, Method: "PUT", Path: "/0/members/1/attributes", Val: `{"name":"member","clientURLs":["http://localhost:2379"]}`},
	} {
		raw, _ := legacy.Marshal()
		wrapped, _ := (&pb.InternalRaftRequest{V2: legacy}).Marshal()
		for _, data := range [][]byte{raw, wrapped} {
			r, err := decodeRequest(data)
			if err != nil {
				t.Fatal(err)
			}
			if requestType(&r) == "unknown" {
				t.Fatal("legacy metadata not adapted")
			}
		}
	}
	unsupported, _ := (&pb.InternalRaftRequest{V2: &pb.Request{Method: "PUT", Path: "/1/user/key", Val: "data"}}).Marshal()
	if _, err := decodeRequest(unsupported); err == nil {
		t.Fatal("legacy user-key writes must not silently replay")
	}
}
