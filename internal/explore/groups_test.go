package explore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
)

func tenantFixture(t *testing.T) string {
	t.Helper()
	run := filepath.Join(t.TempDir(), "run")
	if err := os.Mkdir(run, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := analysis.New(analysis.Config{OutputDir: filepath.Join(run, "analysis"), Window: time.Second, TopN: 1, LabelKeys: []string{"team", "tenant"}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	value := func(kind, ns, name, team, tenant string, withTeam bool, managers []string) []byte {
		labels := map[string]string{}
		if withTeam {
			labels["team"] = team
		}
		if tenant != "" {
			labels["tenant"] = tenant
		}
		meta := map[string]any{"name": name, "namespace": ns, "uid": ns + ":" + name, "labels": labels}
		if kind == "Pod" {
			meta["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "shared", "uid": "owner-" + ns, "controller": true}}
		}
		var fields []any
		for _, manager := range managers {
			fields = append(fields, map[string]any{"manager": manager, "operation": "Apply", "apiVersion": "v1"})
		}
		meta["managedFields"] = fields
		b, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": kind, "metadata": meta})
		return b
	}
	var rev int64
	event := func(kind, key string, v []byte, at time.Duration) {
		t.Helper()
		rev++
		if err := a.Event(analysis.Event{Kind: kind, Key: []byte(key), Value: v, ValueBytes: int64(len(v)), RecordBytes: int64(len(v) + 40), Revision: rev, VirtualNS: int64(at), Index: uint64(rev)}); err != nil {
			t.Fatal(err)
		}
	}
	event("baseline", "/registry/namespaces/alpha", value("Namespace", "", "alpha", "", "red", false, nil), 0)
	event("baseline", "/registry/namespaces/beta", value("Namespace", "", "beta", "", "blue", false, nil), 0)
	event("baseline", "/registry/configmaps/alpha/quiet", value("ConfigMap", "alpha", "quiet", "red", "", true, nil), 0)
	if err := a.Start(analysis.Commit{DBSize: 4096, DBSizeInUse: 4096}); err != nil {
		t.Fatal(err)
	}
	event("put", "/registry/pods/alpha/a", value("Pod", "alpha", "a", "red", "", true, []string{"controller", "kubectl", "controller"}), 100*time.Millisecond)
	event("put", "/registry/pods/beta/b", value("Pod", "beta", "b", "blue", "", true, []string{"kubectl", "controller"}), 200*time.Millisecond)
	event("put", "/registry/configmaps/alpha/missing", value("ConfigMap", "alpha", "missing", "", "", false, nil), 300*time.Millisecond)
	event("put", "/registry/pods/alpha/literal", value("Pod", "alpha", "literal", "(label missing)", "", true, nil), 400*time.Millisecond)
	event("put", "/registry/pods/alpha/empty", value("Pod", "alpha", "empty", "", "", true, nil), 500*time.Millisecond)
	event("put", "/registry/configmaps/beta/encrypted", []byte("k8s:enc:secretbox:v1:synthetic"), 600*time.Millisecond)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(900 * time.Millisecond), DBSize: 16384, DBSizeInUse: 16384}); err != nil {
		t.Fatal(err)
	}
	event("put", "/registry/pods/alpha/a", value("Pod", "alpha", "a", "blue", "", true, []string{"controller"}), 1100*time.Millisecond)
	// The Namespace update follows the object update, within the same window.
	// Classification intentionally uses the Namespace at the window endpoint.
	event("put", "/registry/namespaces/alpha", value("Namespace", "", "alpha", "", "blue", false, nil), 1200*time.Millisecond)
	event("put", "/registry/pods/beta/b", value("Pod", "beta", "b", "blue", "", true, []string{"controller", "kubectl"}), 1300*time.Millisecond)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(1900 * time.Millisecond), DBSize: 24576, DBSizeInUse: 24576}); err != nil {
		t.Fatal(err)
	}
	event("put", "/registry/pods/alpha/a", value("Pod", "alpha", "a", "red", "", true, []string{"kubectl"}), 2100*time.Millisecond)
	if err := a.Commit(analysis.Commit{VirtualNS: int64(2900 * time.Millisecond), DBSize: 32768, DBSizeInUse: 32768}); err != nil {
		t.Fatal(err)
	}
	sum, err := a.Finish(int64(3400 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	writeReport(t, run, sum)
	return run
}

func TestTenantGroupsConserveWritesAndDistinctMissingValues(t *testing.T) {
	s, err := Open(context.Background(), tenantFixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !reflect.DeepEqual(s.Info().LabelKeys, []string{"team", "tenant"}) {
		t.Fatal(s.Info().LabelKeys)
	}
	for _, scope := range []string{"object", "resource", "namespace", "api-group", "kind", "label", "namespace-label", "controller", "manager"} {
		t.Run(scope, func(t *testing.T) {
			q := Query{From: 0, To: 4, Scope: scope, Metric: "writes", LabelKey: "team", Limit: 100}
			r, err := s.Rankings(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			var total int64
			for _, row := range r.Items {
				total += row.Bytes
			}
			if total != r.Totals.Metrics.ValueWriteBytes {
				t.Fatalf("lost or duplicated writes: %d != %d", total, r.Totals.Metrics.ValueWriteBytes)
			}
		})
	}
	r, err := s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "label", Metric: "writes", LabelKey: "team", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var missing, literal *Group
	for _, row := range r.Items {
		if row.Group.Value == "(label missing)" {
			if row.Group.Status == "present" {
				literal = row.Group
			} else {
				missing = row.Group
			}
		}
	}
	if missing == nil || literal == nil || missing.Key == literal.Key {
		t.Fatalf("missing and literal value collision: %+v", r.Items)
	}
	for _, g := range []*Group{missing, literal} {
		d, err := s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "object", Metric: "writes", LabelKey: "team", GroupScope: "label", GroupValue: g.Key, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if d.Total != 1 {
			t.Fatalf("drill group ambiguous: %+v", d)
		}
	}
	empty, err := s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "object", Metric: "writes", LabelKey: "team", LabelFilter: true, LabelValue: "", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Total != 1 || empty.Items[0].Identity.Name != "empty" {
		t.Fatalf("empty label filter: %+v", empty)
	}
}

func TestNamespaceLabelsJoinHistoricalEndpointAndQuietCarry(t *testing.T) {
	s, err := Open(context.Background(), tenantFixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for win, want := range []string{"red", "blue", "blue"} {
		r, err := s.Rankings(context.Background(), Query{From: win, To: win + 1, Scope: "namespace-label", Metric: "writes", LabelKey: "tenant", Namespace: "alpha", Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Items) != 1 || r.Items[0].Group.Value != want {
			t.Fatalf("window %d used future namespace labels or missed quiet carry: %+v", win, r)
		}
		d, err := s.Rankings(context.Background(), Query{From: win, To: win + 1, Scope: "object", Metric: "writes", LabelKey: "tenant", GroupScope: "namespace-label", GroupValue: r.Items[0].Group.Key, Namespace: "alpha", Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		var sum int64
		for _, row := range d.Items {
			sum += row.Bytes
		}
		if sum != r.Items[0].Bytes {
			t.Fatal("drill did not preserve selected group")
		}
	}
	// Namespace filtering also applies to resource aggregation.
	r, err := s.Rankings(context.Background(), Query{From: 0, To: 3, Scope: "resource", Metric: "writes", Namespace: "alpha", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 2 {
		t.Fatalf("namespace resource aggregation: %+v", r)
	}
}

func TestObjectMetadataTimelineAndFilteredNetDeltas(t *testing.T) {
	s, err := Open(context.Background(), tenantFixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	list, err := s.Objects(context.Background(), "/pods/alpha/a", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Items[0].Metadata.Labels["team"] != "red" {
		t.Fatalf("catalog latest metadata: %+v", list)
	}
	d, err := s.Object(context.Background(), list.Items[0].ID, 0, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if d.Points[1].Metadata.Labels["team"] != "red" || d.Points[2].Metadata.Labels["team"] != "blue" || d.Points[3].Metadata.Labels["team"] != "red" || d.Points[4].Metadata.Labels["team"] != "red" {
		t.Fatalf("metadata timeline: %+v", d.Points)
	}
	d1, err := s.Object(context.Background(), list.Items[0].ID, 1, 2, 16)
	if err != nil {
		t.Fatal(err)
	}
	if d1.MetadataStart.Labels["team"] != "red" || d1.Metadata.Labels["team"] != "blue" || d1.MetadataFinal.Labels["team"] != "red" {
		t.Fatal("selected metadata polluted by final inventory")
	}
	var want int64
	for i := 1; i < len(d.Points); i++ {
		if d.Points[i].Metadata.Labels["team"] == "red" {
			want += d.Points[i].Stock.HistoryBytes - d.Points[i-1].Stock.HistoryBytes
		}
	}
	r, err := s.Rankings(context.Background(), Query{From: 0, To: 4, Scope: "object", Metric: "history", Search: "/pods/alpha/a", LabelKey: "team", LabelFilter: true, LabelValue: "red", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 1 || r.Items[0].Bytes != want {
		t.Fatalf("noncontiguous selected rows incorrectly subtracted first/last stock: want %d got %+v", want, r)
	}
	quiet, err := s.Objects(context.Background(), "quiet", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	qd, err := s.Object(context.Background(), quiet.Items[0].ID, 1, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if qd.Metadata.Labels["team"] != "red" || qd.MetadataStart.Labels["team"] != "red" {
		t.Fatal("baseline-only metadata missing")
	}
}

func TestControllerNamespaceIsolationAndManagerSet(t *testing.T) {
	s, err := Open(context.Background(), tenantFixture(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "controller", Metric: "writes", Resource: "pods", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if r.Total != 2 || r.Items[0].Group.Key == r.Items[1].Group.Key {
		t.Fatalf("same named owner in different namespaces collapsed: %+v", r)
	}
	r, err = s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "manager", Metric: "writes", Resource: "pods", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for _, row := range r.Items {
		if row.Group.Value == "controller + kubectl" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("manager order/duplicates create separate groups: %+v", r)
	}
	if !strings.Contains(r.Attribution, "not authenticated writers") {
		t.Fatal("manager attribution boundary absent")
	}
	_, err = s.Rankings(context.Background(), Query{From: 0, To: 1, Scope: "label", Metric: "writes", LabelKey: "not-recorded", Limit: 10})
	if err == nil || !strings.Contains(err.Error(), "--label-key") {
		t.Fatal("missing configured label not explained")
	}
}

func TestLegacyAnalysisMetadataUnavailableAndNewMetadataValidation(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(map[bool]string{true: "legacy", false: "missing-v3-metadata"}[legacy], func(t *testing.T) {
			run := fixture(t)
			data, err := os.ReadFile(filepath.Join(run, "analysis/summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var sum analysis.Summary
			if err = json.Unmarshal(data, &sum); err != nil {
				t.Fatal(err)
			}
			if legacy {
				sum.Version = "object-analysis-v2"
				sum.LabelKeys = nil
			}
			for _, name := range []string{"baseline.objects", "objects", "final.objects"} {
				path := filepath.Join(run, "analysis", name+".jsonl")
				data, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var result strings.Builder
				for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
					if line == "" {
						continue
					}
					var row map[string]any
					if err = json.Unmarshal([]byte(line), &row); err != nil {
						t.Fatal(err)
					}
					delete(row, "metadata")
					b, _ := json.Marshal(row)
					result.Write(b)
					result.WriteByte('\n')
				}
				if err = os.WriteFile(path, []byte(result.String()), 0600); err != nil {
					t.Fatal(err)
				}
				h := sha256.Sum256([]byte(result.String()))
				sum.SHA256[name] = hex.EncodeToString(h[:])
			}
			data, _ = json.Marshal(sum)
			if err = os.WriteFile(filepath.Join(run, "analysis/summary.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			writeReport(t, run, sum)
			s, err := Open(context.Background(), run, "")
			if !legacy {
				if err == nil {
					s.Close()
					t.Fatal("v3 without required metadata accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			list, err := s.Objects(context.Background(), "quiet", "", 10, 0)
			if err != nil {
				t.Fatal(err)
			}
			if list.Items[0].Metadata.Status != "not-recorded" {
				t.Fatalf("legacy metadata status %+v", list)
			}
			_, err = s.Rankings(context.Background(), Query{From: 0, To: 4, Scope: "label", Metric: "writes", LabelKey: "tenant", Limit: 10})
			if err == nil || !strings.Contains(err.Error(), "was not recorded") {
				t.Fatal("legacy label request failed silently")
			}
		})
	}
}
