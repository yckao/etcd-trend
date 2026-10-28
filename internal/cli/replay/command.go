package replaycmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/cli/stdio"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/internal/replay"
)

func Run(ctx context.Context, args []string, streams stdio.Streams) error {
	streams = streams.WithDefaults()
	stdout, stderr := streams.Out, streams.Err
	fs := stdio.Flags("replay", stderr)
	in := fs.String("capture", "", "completed capture directory")
	out := fs.String("out", "", "new output directory (must not exist)")
	speedText := fs.String("speed", "max", "virtual-time pacing: max, 1, 10, 100, ...")
	interval := fs.Duration("batch-interval", 100*time.Millisecond, "modeled backend commit interval")
	limit := fs.Int("batch-limit", 10000, "backend batch operation limit")
	compactLimit := fs.Int("compaction-batch-limit", 1000, "physical compaction batch limit")
	compactSleep := fs.Duration("compaction-sleep", 10*time.Millisecond, "virtual wait between physical compaction batches")
	indexDir := fs.String("index-dir", "", "new disk index directory to retain; default uses a temporary sibling of --out")
	objects := fs.Bool("object-analysis", true, "analyze resource/object contributions during replay")
	window := fs.Duration("window", time.Minute, "object analysis window in virtual source time")
	top := fs.Int("top", 10, "number of ranked contributors retained per window and metric (1-100)")
	var labelKeys []string
	fs.Func("label-key", "label key to extract offline (repeat or comma separate; default extracts no labels)", func(value string) error {
		labelKeys = append(labelKeys, strings.Split(value, ",")...)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *in == "" || *out == "" {
		return fmt.Errorf("usage: etcd-trend replay --capture DIR --out NEW_DIR [--speed max|N]")
	}
	if *window <= 0 || *top < 1 || *top > 100 {
		return fmt.Errorf("window must be positive and top must be 1-100")
	}
	labelKeys, err := analysis.NormalizeLabelKeys(labelKeys)
	if err != nil {
		return err
	}
	var speed float64
	if *speedText != "max" {
		var err error
		speed, err = strconv.ParseFloat(*speedText, 64)
		if err != nil || speed <= 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
			return fmt.Errorf("speed must be max or a finite positive number")
		}
	}
	m, err := capture.ReadManifest(*in)
	if err != nil {
		return err
	}
	if _, err := etcdutil.ParseSourceVersion(m.EtcdVersion); err != nil {
		return err
	}
	if err := validateOutput(*in, *out); err != nil {
		return err
	}
	if *indexDir != "" {
		if err := validateOutput(*in, *indexDir); err != nil {
			return err
		}
		if err := validateSeparatePaths(*out, *indexDir); err != nil {
			return err
		}
		if _, err := os.Lstat(*indexDir); err == nil {
			return fmt.Errorf("index directory already exists: %s", *indexDir)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if _, err := os.Lstat(*out); err == nil {
		return fmt.Errorf("output directory already exists: %s", *out)
	} else if !os.IsNotExist(err) {
		return err
	}
	if *indexDir == "" {
		parent, err := os.MkdirTemp(filepath.Dir(*out), ".etcd-trend-index-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(parent)
		*indexDir = filepath.Join(parent, "index")
	}
	prepareStarted := time.Now()
	d, err := capture.Prepare(ctx, *in, *indexDir)
	if err != nil {
		return err
	}
	prepareDuration := time.Since(prepareStarted)
	stream, err := d.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()
	result, err := replay.Run(ctx, replay.Config{BaselinePath: d.BaselinePath, Source: stream, SourceVersion: d.Manifest.EtcdVersion, OutputDir: *out, Speed: speed, BatchInterval: *interval, BatchLimit: *limit, CompactionBatchLimit: *compactLimit, CompactionSleepInterval: *compactSleep, MemberID: d.Manifest.MemberID, ClusterID: d.Manifest.ClusterID, AnalyzeObjects: *objects, AnalysisWindow: *window, AnalysisTopN: *top, AnalysisLabelKeys: labelKeys})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Output            string `json:"output"`
		Revision          int64  `json:"revision"`
		Applied           int    `json:"applied_entries"`
		Commits           int    `json:"commits"`
		Digest            string `json:"logical_sha256"`
		WallNS            int64  `json:"wall_duration_ns"`
		VirtualNS         int64  `json:"virtual_duration_ns"`
		SourceVersion     string `json:"source_version"`
		Engine            string `json:"engine"`
		PrepareNS         int64  `json:"prepare_duration_ns"`
		IndexBytes        int64  `json:"index_bytes"`
		AnchorBytes       int64  `json:"anchor_index_bytes"`
		AnalysisDirectory string `json:"analysis_directory,omitempty"`
	}{*out, result.FinalRevision, result.AppliedEntries, result.CommitCount, result.LogicalSHA256, result.WallDurationNS, result.VirtualDurationNS, result.SourceVersion, result.Engine, int64(prepareDuration), d.IndexBytes, d.AnchorBytes, result.AnalysisDirectory})
}

func validateOutput(captureDir, outputDir string) error {
	in, err := filepath.EvalSymlinks(captureDir)
	if err != nil {
		return err
	}
	in, err = filepath.Abs(in)
	if err != nil {
		return err
	}
	out, err := canonicalNewPath(outputDir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(in, out)
	if err != nil {
		return err
	}
	if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("output directory must be outside immutable capture")
	}
	return nil
}

func canonicalNewPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func validateSeparatePaths(out, index string) error {
	a, err := canonicalNewPath(out)
	if err != nil {
		return err
	}
	b, err := canonicalNewPath(index)
	if err != nil {
		return err
	}
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("output and index directories must be separate, non-overlapping paths")
		}
	}
	return nil
}
