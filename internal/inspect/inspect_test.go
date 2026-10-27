package inspect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
)

func sampleWindows() []analysis.Window {
	id := analysis.Identity{Key: "/registry/pods/default/leader", KeyEncoding: "utf-8", Resource: "pods", Confidence: "path-layout"}
	rank := analysis.Rank{Identity: id, Bytes: 50}
	ranking := analysis.Rankings{ValueWrites: []analysis.Rank{rank}, HistoryGrowth: []analysis.Rank{rank}, RetainedGrowth: []analysis.Rank{rank}, Compaction: []analysis.Rank{rank}, DBGrowth: []analysis.Rank{rank}, FreeGrowth: []analysis.Rank{rank}, FreeNetGrowth: []analysis.Rank{rank}, HistoryGrowthBytes: 200, RetainedGrowthBytes: 125, FreeNetGrowthBytes: 200}
	return []analysis.Window{
		{StartNS: 0, EndNS: int64(time.Minute), Start: analysis.Stock{RetainedBytes: 100, HistoryBytes: 80}, End: analysis.Stock{RetainedBytes: 90, HistoryBytes: 60}, Metrics: analysis.Metrics{ValueWriteBytes: 100, CompactedBytes: 100, EstimatedDeferredFreeGrowthBytes: 40}, PhysicalStart: analysis.Commit{DBSize: 1000, DBSizeInUse: 900}, PhysicalEnd: analysis.Commit{DBSize: 1100, DBSizeInUse: 900}, CommitCount: 2, DBGrowthBytes: 100, FreeGrowthBytes: 100, UnattributedDBGrowthBytes: 50, UnattributedFreeGrowthBytes: 50, Objects: ranking, Resources: ranking},
		{StartNS: int64(time.Minute), EndNS: int64(2 * time.Minute), PhysicalStart: analysis.Commit{DBSize: 1100, DBSizeInUse: 900}, PhysicalEnd: analysis.Commit{DBSize: 1100, DBSizeInUse: 900}},
	}
}

func fixture(t *testing.T, windows []analysis.Window) (string, string) {
	t.Helper()
	dir := t.TempDir()
	an := filepath.Join(dir, "analysis")
	if err := os.Mkdir(an, 0700); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	encoder := json.NewEncoder(&stream)
	for _, w := range windows {
		if err := encoder.Encode(w); err != nil {
			t.Fatal(err)
		}
	}
	windowPath := filepath.Join(an, "windows.jsonl")
	if err := os.WriteFile(windowPath, stream.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(stream.Bytes())
	s := analysis.Summary{Version: "object-analysis-v2", WindowNS: int64(time.Minute), TopN: 1, WindowCount: int64(len(windows)), Files: map[string]string{"windows": "windows.jsonl"}, SHA256: map[string]string{"windows": hex.EncodeToString(h[:])}}
	writeJSON(t, filepath.Join(an, "summary.json"), s)
	writeJSON(t, filepath.Join(dir, "report.json"), map[string]any{"analysis_directory": "analysis", "analysis": s})
	return dir, windowPath
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func config(dir string) Config {
	return Config{RunDir: dir, Window: -1, Metric: "writes", Scope: "both", Limit: 1}
}

func TestSelectionAndWholeWindowFractions(t *testing.T) {
	dir, _ := fixture(t, sampleWindows())
	for _, tc := range []struct{ metric, fraction string }{{"writes", "50.00%"}, {"history", "25.00%"}, {"retained", "40.00%"}, {"compacted", "50.00%"}, {"db-growth", "50.00%"}, {"free-growth", "50.00%"}, {"free-net", "25.00%"}} {
		t.Run(tc.metric, func(t *testing.T) {
			cfg := config(dir)
			cfg.Window = 0
			cfg.Metric = tc.metric
			cfg.Scope = "object"
			var out bytes.Buffer
			if err := Run(context.Background(), cfg, &out); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			if !strings.Contains(text, tc.fraction) || strings.Contains(text, "100.00%") || strings.Contains(text, "Window 1:") || strings.Contains(text, "RESOURCE ranking") {
				t.Fatalf("wrong selection/denominator:\n%s", text)
			}
			if !strings.Contains(text, "DB bytes: 1000 -> 1100 (+100)") || !strings.Contains(text, "free growth=50") {
				t.Fatalf("missing physical endpoints/unattributed:\n%s", text)
			}
			if !strings.Contains(text, "Estimated free growth associated with previous-commit compaction: 40 bytes") {
				t.Fatalf("missing deferred estimated free growth:\n%s", text)
			}
			if tc.metric == "db-growth" || tc.metric == "free-growth" {
				if !strings.Contains(text, "estimated;") {
					t.Fatal("physical allocation must be labelled estimated")
				}
			}
		})
	}
	cfg := config(dir)
	cfg.Window = 1
	var out bytes.Buffer
	if err := Run(context.Background(), cfg, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Window 0:") || !strings.Contains(out.String(), "Window 1:") {
		t.Fatalf("wrong selected window:\n%s", out.String())
	}
	out.Reset()
	cfg.Window = -1
	if err := Run(context.Background(), cfg, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Window 0:") || !strings.Contains(out.String(), "Window 1:") {
		t.Fatal("all windows were not rendered")
	}
}

func TestInvalidOrTruncatedRunFailsBeforeOutput(t *testing.T) {
	for _, kind := range []string{"malformed", "truncated-line", "truncated-stream", "checksum", "missing-fields", "missing-window", "summary"} {
		t.Run(kind, func(t *testing.T) {
			dir, path := fixture(t, sampleWindows())
			cfg := config(dir)
			cfg.Window = 0
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "malformed":
				data = append(data, []byte("{oops}\n")...)
			case "truncated-line":
				data = data[:len(data)-3]
			case "truncated-stream":
				data = data[:bytes.IndexByte(data, '\n')+1]
			case "checksum":
				data = bytes.Replace(data, []byte("leader"), []byte("reader"), 1)
			case "missing-fields":
				data = append(data, []byte("{}\n")...)
			case "missing-window":
				cfg.Window = 10
			case "summary":
				writeJSON(t, filepath.Join(dir, "analysis", "summary.json"), analysis.Summary{})
			}
			if err = os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err = Run(context.Background(), cfg, &out); err == nil {
				t.Fatal("invalid run accepted")
			}
			if out.Len() != 0 {
				t.Fatalf("rendered incomplete report:\n%s", out.String())
			}
		})
	}
}

func TestArtifactPathsStayInsideRun(t *testing.T) {
	dir, path := fixture(t, sampleWindows())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "windows.jsonl")
	if err = os.WriteFile(external, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(external, path); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = Run(context.Background(), config(dir), &out); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("want path escape error, got %v", err)
	}
}

func TestWriterErrorsPropagate(t *testing.T) {
	dir, _ := fixture(t, sampleWindows())
	if err := Run(context.Background(), config(dir), rejectWriter{}); err == nil {
		t.Fatal("writer error ignored")
	}
}

func TestRejectsPreviousAttributionModel(t *testing.T) {
	dir, _ := fixture(t, sampleWindows())
	summaryPath := filepath.Join(dir, "analysis", "summary.json")
	var summary analysis.Summary
	if err := readJSON(summaryPath, &summary); err != nil {
		t.Fatal(err)
	}
	summary.Version = "object-analysis-v1"
	writeJSON(t, summaryPath, summary)
	writeJSON(t, filepath.Join(dir, "report.json"), map[string]any{"analysis_directory": "analysis", "analysis": summary})
	var out bytes.Buffer
	if err := Run(context.Background(), config(dir), &out); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported model error, got %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("unsupported model emitted a report")
	}
}

type rejectWriter struct{}

func (rejectWriter) Write([]byte) (int, error) { return 0, os.ErrPermission }
