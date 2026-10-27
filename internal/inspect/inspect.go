// Package inspect renders completed replay object-analysis windows without
// retaining the full timeline or opening an etcd connection.
package inspect

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
)

type Config struct {
	RunDir string
	Window int64 // -1 selects every window; otherwise zero based.
	Metric string
	Scope  string
	Limit  int
}

// Run validates the completed report and the entire windows stream before
// rendering. Both validation and rendering keep one window in memory. An
// explicit window still validates the remaining stream, so a truncated report
// cannot be presented as a complete successful run.
func Run(ctx context.Context, cfg Config, output io.Writer) error {
	if cfg.RunDir == "" || cfg.Window < -1 || cfg.Limit < 1 || cfg.Limit > 100 {
		return fmt.Errorf("run directory, window >=0 (or all), and limit 1-100 required")
	}
	if cfg.Scope != "both" && cfg.Scope != "object" && cfg.Scope != "resource" {
		return fmt.Errorf("scope must be both, resource, or object")
	}
	if _, ok := metricLabels[cfg.Metric]; !ok {
		return fmt.Errorf("metric must be writes, history, retained, compacted, db-growth, free-growth, or free-net")
	}
	root, err := filepath.EvalSymlinks(cfg.RunDir)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	reportPath, err := confined(root, "report.json")
	if err != nil {
		return err
	}
	var report struct {
		AnalysisDirectory string            `json:"analysis_directory"`
		Analysis          *analysis.Summary `json:"analysis"`
	}
	if err = readJSON(reportPath, &report); err != nil {
		return fmt.Errorf("completed replay report: %w", err)
	}
	if report.AnalysisDirectory != "analysis" || report.Analysis == nil {
		return fmt.Errorf("run has no completed object analysis in the supported analysis directory")
	}
	summaryPath, err := confined(root, "analysis/summary.json")
	if err != nil {
		return err
	}
	var summary analysis.Summary
	if err = readJSON(summaryPath, &summary); err != nil {
		return fmt.Errorf("analysis summary: %w", err)
	}
	if !reflect.DeepEqual(summary, *report.Analysis) {
		return fmt.Errorf("analysis summary differs from completed replay report")
	}
	if (summary.Version != "object-analysis-v2" && summary.Version != "object-analysis-v3") || summary.WindowNS <= 0 || summary.WindowCount < 1 || summary.TopN < 1 || summary.TopN > 100 {
		return fmt.Errorf("unsupported or incomplete object analysis summary")
	}
	if summary.Files["windows"] != "windows.jsonl" || len(summary.SHA256["windows"]) != 64 {
		return fmt.Errorf("analysis summary lacks the expected windows file and checksum")
	}
	if cfg.Window >= summary.WindowCount {
		return fmt.Errorf("window %d not found; available windows are 0 through %d", cfg.Window, summary.WindowCount-1)
	}
	windowsPath, err := confined(root, "analysis/windows.jsonl")
	if err != nil {
		return err
	}
	f, err := os.Open(windowsPath)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	count, err := walkWindows(ctx, io.TeeReader(f, h), nil)
	if err != nil {
		return err
	}
	if count != summary.WindowCount {
		return fmt.Errorf("windows count mismatch: got %d, expected %d", count, summary.WindowCount)
	}
	if hex.EncodeToString(h.Sum(nil)) != summary.SHA256["windows"] {
		return fmt.Errorf("windows checksum mismatch")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(output, "Metric: %s\n", metricLabels[cfg.Metric]); err != nil {
		return err
	}
	if cfg.Limit > summary.TopN {
		if _, err = fmt.Fprintf(output, "Stored ranking limit: %d; at most %d rows per scope are available.\n", summary.TopN, summary.TopN); err != nil {
			return err
		}
	}
	_, err = walkWindows(ctx, f, func(n int64, w analysis.Window) error {
		if cfg.Window >= 0 && n != cfg.Window {
			return nil
		}
		return renderWindow(output, cfg, n, w)
	})
	return err
}

func confined(root, relative string) (string, error) {
	path, err := filepath.EvalSymlinks(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("analysis artifact escapes replay directory: %s", relative)
	}
	return path, nil
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// Summary/report are constant-size metadata. Refuse accidental payload files.
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return fmt.Errorf("metadata must be a regular file no larger than 1 MiB")
	}
	d := json.NewDecoder(f)
	if err = d.Decode(v); err != nil {
		return err
	}
	var trailing any
	if err = d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unexpected trailing JSON data")
	}
	return nil
}

func walkWindows(ctx context.Context, source io.Reader, visit func(int64, analysis.Window) error) (int64, error) {
	scanner := bufio.NewScanner(source)
	// A window holds six rankings, at most100 identities each, in two scopes.
	// Bound malformed input independently of total recording duration.
	scanner.Buffer(make([]byte, 64<<10), 64<<20)
	var n, previousEnd int64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		var w analysis.Window
		if len(scanner.Bytes()) == 0 {
			return n, fmt.Errorf("empty windows record at line %d", n+1)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &fields); err != nil {
			return n, fmt.Errorf("window %d: %w", n, err)
		}
		for _, name := range []string{"start_ns", "end_ns", "start", "end", "metrics", "physical_start", "physical_end", "commit_count", "top_objects", "top_resources"} {
			if raw, ok := fields[name]; !ok || string(raw) == "null" {
				return n, fmt.Errorf("window %d lacks required field %s", n, name)
			}
		}
		if err := json.Unmarshal(scanner.Bytes(), &w); err != nil {
			return n, fmt.Errorf("window %d: %w", n, err)
		}
		if w.StartNS != previousEnd || w.EndNS < w.StartNS {
			return n, fmt.Errorf("window %d has a noncontiguous or invalid time range", n)
		}
		if visit != nil {
			if err := visit(n, w); err != nil {
				return n, err
			}
		}
		previousEnd = w.EndNS
		n++
	}
	if err := scanner.Err(); err != nil {
		return n, fmt.Errorf("read windows: %w", err)
	}
	return n, nil
}

var metricLabels = map[string]string{
	"writes":      "value bytes written (measured)",
	"history":     "positive retained-history growth (measured MVCC bytes)",
	"retained":    "positive retained-record growth (measured MVCC bytes)",
	"compacted":   "MVCC record bytes physically removed by compaction (measured)",
	"db-growth":   "DB growth association (estimated; share of observed gross DB growth)",
	"free-growth": "free-space growth association (estimated; share of observed gross free-space growth)",
	"free-net":    "positive net free-space association after reuse (estimated; not unique page ownership)",
}

func renderWindow(output io.Writer, cfg Config, n int64, w analysis.Window) error {
	startFree := w.PhysicalStart.DBSize - w.PhysicalStart.DBSizeInUse
	endFree := w.PhysicalEnd.DBSize - w.PhysicalEnd.DBSizeInUse
	if _, err := fmt.Fprintf(output, "\nWindow %d: source virtual time %s -> %s; commits=%d\nDB bytes: %d -> %d (%+d)\nIn-use bytes: %d -> %d (%+d)\nFree bytes: %d -> %d (%+d); gross created=%d, reused=%d\nMVCC retained bytes: %d -> %d (%+d); history: %d -> %d (%+d)\nUnattributed bytes: DB growth=%d, free growth=%d, free reuse=%d\n",
		n, time.Duration(w.StartNS), time.Duration(w.EndNS), w.CommitCount,
		w.PhysicalStart.DBSize, w.PhysicalEnd.DBSize, w.PhysicalEnd.DBSize-w.PhysicalStart.DBSize,
		w.PhysicalStart.DBSizeInUse, w.PhysicalEnd.DBSizeInUse, w.PhysicalEnd.DBSizeInUse-w.PhysicalStart.DBSizeInUse,
		startFree, endFree, endFree-startFree, w.FreeGrowthBytes, w.FreeReuseBytes,
		w.Start.RetainedBytes, w.End.RetainedBytes, w.End.RetainedBytes-w.Start.RetainedBytes,
		w.Start.HistoryBytes, w.End.HistoryBytes, w.End.HistoryBytes-w.Start.HistoryBytes,
		w.UnattributedDBGrowthBytes, w.UnattributedFreeGrowthBytes, w.UnattributedFreeReuseBytes); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "Estimated free growth associated with previous-commit compaction: %d bytes\n", w.Metrics.EstimatedDeferredFreeGrowthBytes); err != nil {
		return err
	}
	for _, scope := range []struct {
		name    string
		ranking analysis.Rankings
	}{{"resource", w.Resources}, {"object", w.Objects}} {
		if cfg.Scope != "both" && cfg.Scope != scope.name {
			continue
		}
		ranks, denominator, denominatorLabel := selectMetric(cfg.Metric, scope.ranking, w)
		if _, err := fmt.Fprintf(output, "%s ranking; denominator=%d bytes (%s)\n", strings.ToUpper(scope.name), denominator, denominatorLabel); err != nil {
			return err
		}
		t := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(t, "RANK\tIDENTITY\tBYTES\tWINDOW SHARE\tMAPPING"); err != nil {
			return err
		}
		if len(ranks) > cfg.Limit {
			ranks = ranks[:cfg.Limit]
		}
		for i, rank := range ranks {
			label := rank.Identity.Resource
			if scope.name == "object" {
				label = rank.Identity.Key
				if rank.Identity.KeyEncoding == "base64" {
					label = "base64:" + label
				}
			}
			fraction := "n/a"
			if denominator > 0 {
				fraction = fmt.Sprintf("%.2f%%", 100*float64(rank.Bytes)/float64(denominator))
			}
			if _, err := fmt.Fprintf(t, "%d\t%s\t%d\t%s\t%s\n", i+1, strconv.QuoteToASCII(label), rank.Bytes, fraction, rank.Identity.Confidence); err != nil {
				return err
			}
		}
		if len(ranks) == 0 {
			if _, err := fmt.Fprintln(t, "-\t(no positive contributors)\t0\t-\t-"); err != nil {
				return err
			}
		}
		if err := t.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func selectMetric(metric string, r analysis.Rankings, w analysis.Window) ([]analysis.Rank, int64, string) {
	switch metric {
	case "writes":
		return r.ValueWrites, w.Metrics.ValueWriteBytes, "all value writes in window"
	case "history":
		return r.HistoryGrowth, r.HistoryGrowthBytes, "all positive row history deltas in this scope; excludes decreases"
	case "retained":
		return r.RetainedGrowth, r.RetainedGrowthBytes, "all positive row retained deltas in this scope; excludes decreases"
	case "compacted":
		return r.Compaction, w.Metrics.CompactedBytes, "all compacted MVCC bytes in window"
	case "db-growth":
		return r.DBGrowth, w.DBGrowthBytes, "all observed DB growth, including unattributed"
	case "free-net":
		return r.FreeNetGrowth, r.FreeNetGrowthBytes, "all positive attributed net free deltas in this scope; excludes decreases and unattributed changes"
	default:
		return r.FreeGrowth, w.FreeGrowthBytes, "all observed free-space growth, including unattributed"
	}
}
