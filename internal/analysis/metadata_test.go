package analysis

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMetadataFromUpstreamKubernetesSerializers(t *testing.T) {
	var results []Metadata
	for _, format := range []string{"json", "pb"} {
		value, err := os.ReadFile(filepath.Join("testdata", "upstream-pod."+format))
		if err != nil {
			t.Fatal(err)
		}
		m := DecodeMetadata(value, []string{"tenant", "app.kubernetes.io/name", "absent"})
		status := "decoded-json"
		if format == "pb" {
			status = "decoded-protobuf"
		}
		if m.Status != status || m.APIVersion != "v1" || m.APIGroup != "" || m.Kind != "Pod" || m.Name != "worker-0" || m.Namespace != "tenant-blue" || m.UID != "pod-uid-001" {
			t.Fatalf("bad %s metadata: %+v", format, m)
		}
		if !reflect.DeepEqual(m.Labels, map[string]string{"tenant": "blue", "app.kubernetes.io/name": "orders"}) || !reflect.DeepEqual(m.Managers, []string{"kubelet", "rollout-controller"}) || m.Controller == nil || *m.Controller != (OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "orders-abc", UID: "replicaset-uid-001"}) {
			t.Fatalf("bad attribution fields: %+v", m)
		}
		data, _ := json.Marshal(m)
		for _, excluded := range []string{"do-not-export", "annotation-not-exported", "spec-not-exported", "fieldsV1", "example.invalid"} {
			if bytes.Contains(data, []byte(excluded)) {
				t.Fatalf("exported %q", excluded)
			}
		}
		if all := DecodeMetadata(value, nil); len(all.Labels) != 0 {
			t.Fatal("labels exported without an allowlist")
		}
		m.Status = ""
		results = append(results, m)
	}
	if !reflect.DeepEqual(results[0], results[1]) {
		t.Fatalf("JSON/protobuf metadata differs: %+v vs %+v", results[0], results[1])
	}
}

func TestMetadataCoverageStatusesAndMalformedValues(t *testing.T) {
	for _, tc := range []struct{ value, status string }{
		{"k8s:enc:aescbc:v1:key:ciphertext", "encrypted"},
		{"raw opaque value", "opaque"},
		{"{}", "opaque"},
		{"{\"kind\":", "malformed"},
		{"k8s\x00", "malformed"},
		{"k8s\x00\x12\xff", "malformed"},
		{"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"p\",\"labels\":{\"tenant\":1}}}", "malformed"},
		{"{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"\xff\"}}", "malformed"},
	} {
		m := DecodeMetadata([]byte(tc.value), []string{"tenant"})
		if m.Status != tc.status || m.Name != "" || m.Labels != nil {
			t.Fatalf("%q: %+v, want %s without partial metadata", tc.value, m, tc.status)
		}
	}
	value, err := os.ReadFile(filepath.Join("testdata", "upstream-pod.pb"))
	if err != nil {
		t.Fatal(err)
	}
	// Any truncated framing must remain safe, including truncation at a valid
	// message boundary. Some shorter protobufs are still valid partial objects.
	for i := range value {
		_ = DecodeMetadata(value[:i], []string{"tenant"})
	}
	m := DecodeMetadata([]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","labels":{"tenant":""}}}`), []string{"tenant"})
	if m.APIGroup != "apps" || m.Labels["tenant"] != "" || len(m.Labels) != 1 {
		t.Fatalf("group or present-empty label lost: %+v", m)
	}
}

func TestMetadataWindowEndpointsAndRecreation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "analysis")
	a, err := New(Config{OutputDir: dir, Window: time.Second, LabelKeys: []string{"tenant", "tenant"}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	key := []byte("/registry/configmaps/ns/item")
	value := func(tenant, uid string) []byte {
		return []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"item","namespace":"ns","uid":"` + uid + `","labels":{"tenant":"` + tenant + `","private":"do-not-export"}},"data":{"secret":"spec-not-exported"}}`)
	}
	event := func(kind string, rev int64, at time.Duration, tenant, uid string) {
		t.Helper()
		tombstone := kind == "delete" || rev == 4
		v := value(tenant, uid)
		if tombstone {
			v = nil
		}
		if err := a.Event(Event{Kind: kind, Key: key, Value: v, Revision: rev, RecordBytes: int64(len(v) + 30), ValueBytes: int64(len(v)), Tombstone: tombstone, Index: uint64(rev), VirtualNS: int64(at)}); err != nil {
			t.Fatal(err)
		}
	}
	event("baseline", 1, 0, "blue", "first")
	if err := a.Start(Commit{DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	event("put", 2, 100*time.Millisecond, "green", "first")
	event("put", 3, 200*time.Millisecond, "red", "first")
	event("compact", 1, 300*time.Millisecond, "blue", "first")
	if err := a.Commit(Commit{VirtualNS: int64(400 * time.Millisecond), DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	event("delete", 4, 1100*time.Millisecond, "", "")
	event("compact", 2, 1200*time.Millisecond, "green", "first")
	event("compact", 3, 1300*time.Millisecond, "red", "first")
	event("compact", 4, 1400*time.Millisecond, "", "")
	if err := a.Commit(Commit{VirtualNS: int64(1500 * time.Millisecond), DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	event("put", 5, 2100*time.Millisecond, "blue", "second")
	if err := a.Commit(Commit{VirtualNS: int64(2500 * time.Millisecond), DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	summary, err := a.Finish(int64(2800 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if summary.Version != "object-analysis-v3" || !reflect.DeepEqual(summary.LabelKeys, []string{"tenant"}) {
		t.Fatalf("missing metadata contract: %+v", summary)
	}
	rows := readJSONL[Row](t, filepath.Join(dir, "objects.jsonl"))
	if len(rows) != 3 || rows[0].Metadata.Labels["tenant"] != "red" || rows[1].Metadata.Labels["tenant"] != "red" || rows[2].Metadata.Labels["tenant"] != "blue" || rows[2].Metadata.UID != "second" || rows[1].Metadata.Revision != 3 || rows[1].End.RetainedBytes != 0 {
		t.Fatalf("metadata went backwards during compaction/deletion: %+v", rows)
	}
	if rows[0].Metadata.IdentityMismatch || rows[0].Metrics.Puts != 2 || rows[1].Metrics.CompactedRecords != 3 {
		t.Fatalf("metadata changed accounting or identity: %+v", rows)
	}
	type inventory struct {
		Metadata Metadata `json:"metadata"`
	}
	baseline := readJSONL[inventory](t, filepath.Join(dir, "baseline.objects.jsonl"))
	final := readJSONL[inventory](t, filepath.Join(dir, "final.objects.jsonl"))
	if baseline[0].Metadata.Labels["tenant"] != "blue" || baseline[0].Metadata.UID != "first" || final[0].Metadata.UID != "second" {
		t.Fatal("inventory metadata is not scoped to its boundary")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("do-not-export")) || bytes.Contains(data, []byte("spec-not-exported")) {
			t.Fatalf("payload leaked into %s", filepath.Base(file))
		}
	}
}

func TestLabelAllowlistValidation(t *testing.T) {
	got, err := NormalizeLabelKeys([]string{" tenant ", "app.kubernetes.io/name", "tenant", "team_name", "prefix/name"})
	if err != nil || !reflect.DeepEqual(got, []string{"app.kubernetes.io/name", "prefix/name", "team_name", "tenant"}) {
		t.Fatalf("normalization: %v, %v", got, err)
	}
	for _, invalid := range []string{"", "/key", "a/b/c", "name=private", "team name", ".name", "-name", strings.Repeat("x", 64)} {
		if _, err := NormalizeLabelKeys([]string{invalid}); err == nil {
			t.Fatalf("accepted invalid label key %q", invalid)
		}
	}
}
