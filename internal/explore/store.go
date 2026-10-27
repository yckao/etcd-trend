package explore

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	bolt "go.etcd.io/bbolt"
)

var bucketWindows = []byte("windows")
var bucketObjects = []byte("objects")
var bucketResources = []byte("resources")
var bucketObjectRows = []byte("object_rows")
var bucketResourceRows = []byte("resource_rows")
var bucketHistory = []byte("history")
var bucketNamespaces = []byte("namespaces")

type Store struct {
	mu          sync.RWMutex
	db          *bolt.DB
	info        Info
	cache       string
	removeCache bool
	closed      bool
}

type catalog struct {
	ObjectItem
	HasBaseline      bool              `json:"has_baseline"`
	HasFinal         bool              `json:"has_final"`
	HasRows          bool              `json:"has_rows"`
	Latest           analysis.Stock    `json:"latest"`
	BaselineMetadata analysis.Metadata `json:"baseline_metadata"`
	FinalMetadata    analysis.Metadata `json:"final_metadata"`
}

type inventory struct {
	Identity analysis.Identity `json:"identity"`
	Stock    analysis.Stock    `json:"stock"`
	Metadata analysis.Metadata `json:"metadata"`
}

// Open validates every consumed analysis file before publishing an index. The
// input is never opened for writing. A caller-supplied cache must be a new path.
func Open(ctx context.Context, runDir, cacheDir string) (_ *Store, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	root, err := filepath.EvalSymlinks(runDir)
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	var report struct {
		AnalysisDirectory    string            `json:"analysis_directory"`
		Analysis             *analysis.Summary `json:"analysis"`
		SourceVersion        string            `json:"source_version"`
		Engine               string            `json:"engine"`
		FinalRevision        int64             `json:"final_revision"`
		LogicalSHA256        string            `json:"logical_sha256"`
		BatchIntervalNS      int64             `json:"batch_interval_ns"`
		BatchLimit           int               `json:"batch_limit"`
		CompactionBatchLimit int               `json:"compaction_batch_limit"`
		CompactionSleepNS    int64             `json:"compaction_sleep_ns"`
	}
	path, err := confined(root, "report.json")
	if err != nil {
		return nil, err
	}
	if err = readMetadata(path, &report); err != nil {
		return nil, fmt.Errorf("completed replay report: %w", err)
	}
	if report.AnalysisDirectory != "analysis" || report.Analysis == nil {
		return nil, fmt.Errorf("run has no completed object analysis")
	}
	path, err = confined(root, "analysis/summary.json")
	if err != nil {
		return nil, err
	}
	var summary analysis.Summary
	if err = readMetadata(path, &summary); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(summary, *report.Analysis) {
		return nil, fmt.Errorf("analysis summary differs from completed replay report")
	}
	if (summary.Version != "object-analysis-v2" && summary.Version != "object-analysis-v3") || summary.WindowNS <= 0 || summary.WindowCount < 1 || summary.WindowCount > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("unsupported analysis summary")
	}
	if _, err = analysis.NormalizeLabelKeys(summary.LabelKeys); err != nil {
		return nil, fmt.Errorf("invalid recorded label keys: %w", err)
	}
	s := &Store{info: Info{SourceVersion: report.SourceVersion, Engine: report.Engine, AnalysisVersion: summary.Version, Model: summary.Model, WindowCount: int(summary.WindowCount), WindowNS: summary.WindowNS, FinalRevision: report.FinalRevision, LogicalSHA256: report.LogicalSHA256, Parameters: map[string]any{"batch_interval_ns": report.BatchIntervalNS, "batch_limit": report.BatchLimit, "compaction_batch_limit": report.CompactionBatchLimit, "compaction_sleep_ns": report.CompactionSleepNS}}}
	s.info.LabelKeys = append([]string{}, summary.LabelKeys...)
	if cacheDir == "" {
		var parent string
		parent, err = filepath.EvalSymlinks(os.TempDir())
		if err == nil {
			parent, err = filepath.Abs(parent)
		}
		if err == nil && within(root, parent) {
			err = fmt.Errorf("temporary directory lies inside replay; choose an external cache directory")
		}
		if err == nil {
			s.cache, err = os.MkdirTemp(parent, "etcd-trend-explore-")
		}
		s.removeCache = true
	} else {
		s.cache, err = canonicalNew(cacheDir)
		if err == nil && within(root, s.cache) {
			err = fmt.Errorf("cache must be outside the immutable replay directory")
		}
		if err == nil {
			err = os.Mkdir(s.cache, 0700)
		}
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if s.db != nil {
				_ = s.db.Close()
			}
			_ = os.RemoveAll(s.cache)
		}
	}()
	w, err := newWriter(filepath.Join(s.cache, "index.db"), bucketWindows, bucketObjects, bucketResources, bucketObjectRows, bucketResourceRows, bucketHistory, bucketNamespaces)
	if err != nil {
		return nil, err
	}
	defer w.Close()
	var windowCount, lastEnd int64
	err = scanVerified(ctx, root, summary, "windows", func(data []byte) error {
		if err := requireFields(data, "start_ns", "end_ns", "start", "end", "metrics", "physical_start", "physical_end", "commit_count"); err != nil {
			return err
		}
		var v analysis.Window
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		if v.StartNS != lastEnd || v.EndNS < v.StartNS || v.StartNS != windowCount*summary.WindowNS {
			return fmt.Errorf("invalid window sequence at %d", windowCount)
		}
		if v.PhysicalStart.DBSize < v.PhysicalStart.DBSizeInUse || v.PhysicalEnd.DBSize < v.PhysicalEnd.DBSizeInUse || v.PhysicalStart.DBSizeInUse < 0 || v.PhysicalEnd.DBSizeInUse < 0 {
			return fmt.Errorf("invalid physical window")
		}
		v.Objects = analysis.Rankings{}
		v.Resources = analysis.Rankings{}
		if err := w.putJSON(bucketWindows, numberKey(int(windowCount)), v, true); err != nil {
			return err
		}
		lastEnd = v.EndNS
		windowCount++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if windowCount != summary.WindowCount {
		return nil, fmt.Errorf("window count mismatch")
	}
	s.info.DurationNS = lastEnd
	for _, item := range []struct {
		name     string
		bucket   []byte
		baseline bool
		expected int64
	}{
		{"baseline.objects", bucketObjects, true, summary.BaselineObjects}, {"baseline.resources", bucketResources, true, -1},
		{"final.objects", bucketObjects, false, summary.FinalObjects}, {"final.resources", bucketResources, false, -1},
	} {
		var count int64
		err = scanVerified(ctx, root, summary, item.name, func(data []byte) error {
			if err := requireFields(data, "identity", "stock"); err != nil {
				return err
			}
			var v inventory
			if err := json.Unmarshal(data, &v); err != nil {
				return err
			}
			object := string(item.bucket) == string(bucketObjects)
			if object && summary.Version == "object-analysis-v3" {
				if err := requireFields(data, "metadata"); err != nil {
					return err
				}
				if err := validateMetadata(v.Metadata, summary.LabelKeys); err != nil {
					return err
				}
			}
			if err := validateIdentity(v.Identity, object); err != nil {
				return err
			}
			id := identityID(v.Identity, object)
			var old catalog
			if err := getJSON(w.tx.Bucket(item.bucket), []byte(id), &old); err != nil {
				return err
			}
			if item.baseline && old.HasBaseline || !item.baseline && old.HasFinal {
				return fmt.Errorf("duplicate inventory identity")
			}
			if old.ID != "" && !reflect.DeepEqual(old.Identity, v.Identity) {
				return fmt.Errorf("inventory identity changed")
			}
			old.ID, old.Identity = id, v.Identity
			if item.baseline {
				old.Baseline = v.Stock
				old.BaselineMetadata = metadataOrUnknown(v.Metadata)
				old.Metadata = old.BaselineMetadata
				old.HasBaseline = true
			} else {
				old.Final = v.Stock
				old.FinalMetadata = metadataOrUnknown(v.Metadata)
				old.Metadata = old.FinalMetadata
				old.HasFinal = true
			}
			if object {
				if err := indexNamespace(w, v.Identity, v.Metadata, id); err != nil {
					return err
				}
			}
			if err := w.putJSON(item.bucket, []byte(id), old, false); err != nil {
				return err
			}
			count++
			return nil
		})
		if err != nil {
			return nil, err
		}
		if item.expected >= 0 && count != item.expected {
			return nil, fmt.Errorf("%s count mismatch", item.name)
		}
	}
	for _, item := range []struct {
		name           string
		rows, entities []byte
		object         bool
		expected       int64
	}{{"objects", bucketObjectRows, bucketObjects, true, summary.ObjectRows}, {"resources", bucketResourceRows, bucketResources, false, summary.ResourceRows}} {
		var count int64
		err = scanVerified(ctx, root, summary, item.name, func(data []byte) error {
			if err := requireFields(data, "window_start_ns", "window_end_ns", "identity", "start", "end", "metrics"); err != nil {
				return err
			}
			var r analysis.Row
			if err := json.Unmarshal(data, &r); err != nil {
				return err
			}
			if item.object && summary.Version == "object-analysis-v3" {
				if err := requireFields(data, "metadata"); err != nil {
					return err
				}
				if err := validateMetadata(r.Metadata, summary.LabelKeys); err != nil {
					return err
				}
			}
			r.Metadata = metadataOrUnknown(r.Metadata)
			if err := validateIdentity(r.Identity, item.object); err != nil {
				return err
			}
			if r.WindowStartNS < 0 || r.WindowStartNS%summary.WindowNS != 0 {
				return fmt.Errorf("invalid row window")
			}
			if r.WindowStartNS/summary.WindowNS >= summary.WindowCount {
				return fmt.Errorf("row refers to invalid window")
			}
			win := int(r.WindowStartNS / summary.WindowNS)
			var v analysis.Window
			if err := getJSON(w.tx.Bucket(bucketWindows), numberKey(win), &v); err != nil {
				return err
			}
			if win >= s.info.WindowCount || r.WindowEndNS != v.EndNS {
				return fmt.Errorf("row refers to invalid window")
			}
			id := identityID(r.Identity, item.object)
			if err := w.putJSON(item.rows, rowKey(win, id), r, true); err != nil {
				return err
			}
			if item.object {
				if err := w.putJSON(bucketHistory, historyKey(id, win), r, true); err != nil {
					return err
				}
				if err := indexNamespace(w, r.Identity, r.Metadata, id); err != nil {
					return err
				}
			}
			var c catalog
			if err := getJSON(w.tx.Bucket(item.entities), []byte(id), &c); err != nil {
				return err
			}
			if c.ID == "" {
				c.ID, c.Identity = id, r.Identity
			} else if !reflect.DeepEqual(c.Identity, r.Identity) {
				return fmt.Errorf("row identity changed")
			}
			previous := c.Baseline
			if c.HasRows {
				previous = c.Latest
			}
			if previous != r.Start {
				return fmt.Errorf("row stock continuity mismatch for %s", id)
			}
			c.HasRows, c.Latest = true, r.End
			c.Metadata = r.Metadata
			if err := w.putJSON(item.entities, []byte(id), c, false); err != nil {
				return err
			}
			count++
			return nil
		})
		if err != nil {
			return nil, err
		}
		if count != item.expected {
			return nil, fmt.Errorf("%s row count mismatch", item.name)
		}
	}
	if err = w.finish(); err != nil {
		return nil, err
	}
	s.db, err = bolt.Open(filepath.Join(s.cache, "index.db"), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		s.info.ObjectCount = int64(tx.Bucket(bucketObjects).Stats().KeyN)
		s.info.ResourceCount = int64(tx.Bucket(bucketResources).Stats().KeyN)
		for _, bucket := range [][]byte{bucketObjects, bucketResources} {
			if err := tx.Bucket(bucket).ForEach(func(k, v []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				var c catalog
				if err := json.Unmarshal(v, &c); err != nil {
					return err
				}
				expected := c.Baseline
				if c.HasRows {
					expected = c.Latest
				}
				if expected != c.Final || (expected.RetainedRecords > 0) != c.HasFinal {
					return fmt.Errorf("final inventory differs from sparse rows for %s", c.ID)
				}
				if string(bucket) == string(bucketObjects) && c.HasFinal && summary.Version == "object-analysis-v3" {
					metadata := c.BaselineMetadata
					if c.HasRows {
						metadata = c.Metadata
					}
					if !reflect.DeepEqual(metadata, c.FinalMetadata) {
						return fmt.Errorf("final metadata differs from sparse rows for %s", c.ID)
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Info() Info {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := s.info
	v.LabelKeys = append([]string{}, s.info.LabelKeys...)
	v.Parameters = map[string]any{}
	for k, x := range s.info.Parameters {
		v.Parameters[k] = x
	}
	return v
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.db.Close()
	if s.removeCache {
		err = errors.Join(err, os.RemoveAll(s.cache))
	}
	return err
}
func (s *Store) checkRange(from, to int) error {
	if s.closed {
		return fmt.Errorf("explorer is closed")
	}
	if from < 0 || to <= from || to > s.info.WindowCount {
		return fmt.Errorf("window range must satisfy 0 <= from < to <= %d", s.info.WindowCount)
	}
	return nil
}

func identityID(id analysis.Identity, object bool) string {
	var b []byte
	if object {
		b, _ = json.Marshal([]string{id.KeyEncoding, id.Key})
	} else {
		b, _ = json.Marshal([]string{id.Resource, id.APIGroup})
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validateIdentity(id analysis.Identity, object bool) error {
	if id.Resource == "" {
		return fmt.Errorf("missing resource identity")
	}
	if object && (id.Key == "" || (id.KeyEncoding != "utf-8" && id.KeyEncoding != "base64")) {
		return fmt.Errorf("invalid object identity")
	}
	return nil
}
func numberKey(n int) []byte               { var k [8]byte; binary.BigEndian.PutUint64(k[:], uint64(n)); return k[:] }
func rowKey(win int, id string) []byte     { return append(numberKey(win), []byte(id)...) }
func historyKey(id string, win int) []byte { return append(append([]byte(id), 0), numberKey(win)...) }
func getJSON(b *bolt.Bucket, k []byte, v any) error {
	if data := b.Get(k); data != nil {
		return json.Unmarshal(data, v)
	}
	return nil
}
func canonicalNew(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func confined(root, rel string) (string, error) {
	p, err := filepath.EvalSymlinks(filepath.Join(root, rel))
	if err != nil {
		return "", err
	}
	if !within(root, p) {
		return "", fmt.Errorf("artifact escapes replay directory: %s", rel)
	}
	return p, nil
}
func readMetadata(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() > 1<<20 {
		return fmt.Errorf("invalid metadata file")
	}
	d := json.NewDecoder(f)
	if err = d.Decode(v); err != nil {
		return err
	}
	var trailing any
	if err = d.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing metadata JSON")
	}
	return nil
}

func requireFields(data []byte, names ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range names {
		v, ok := fields[name]
		if !ok || string(v) == "null" {
			return fmt.Errorf("missing required field %s", name)
		}
	}
	return nil
}

func scanVerified(ctx context.Context, root string, s analysis.Summary, name string, visit func([]byte) error) error {
	if s.Files[name] != name+".jsonl" || len(s.SHA256[name]) != 64 {
		return fmt.Errorf("invalid %s file reference", name)
	}
	p, err := confined(root, "analysis/"+s.Files[name])
	if err != nil {
		return err
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", name)
	}
	h := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(f, h))
	scanner.Buffer(make([]byte, 64<<10), 64<<20)
	var line int64
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return err
		}
		line++
		if len(scanner.Bytes()) == 0 {
			return fmt.Errorf("empty %s record", name)
		}
		if err = visit(scanner.Bytes()); err != nil {
			return fmt.Errorf("%s line %d: %w", name, line, err)
		}
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != s.SHA256[name] {
		return fmt.Errorf("%s checksum mismatch", name)
	}
	return nil
}

type writer struct {
	db    *bolt.DB
	tx    *bolt.Tx
	dirty int
}

func newWriter(path string, buckets ...[]byte) (w *writer, err error) {
	w = &writer{}
	created := w
	w.db, err = bolt.Open(path, 0600, &bolt.Options{NoSync: true, Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			created.Close()
		}
	}()
	w.tx, err = w.db.Begin(true)
	if err != nil {
		return nil, err
	}
	for _, b := range buckets {
		if _, err = w.tx.CreateBucket(b); err != nil {
			return nil, err
		}
	}
	return w, nil
}
func (w *writer) putJSON(bucket, key []byte, value any, unique bool) error {
	b := w.tx.Bucket(bucket)
	if unique && b.Get(key) != nil {
		return fmt.Errorf("duplicate indexed record")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err = b.Put(key, raw); err != nil {
		return err
	}
	w.dirty++
	if w.dirty >= 2048 {
		return w.checkpoint()
	}
	return nil
}
func (w *writer) checkpoint() error {
	if err := w.tx.Commit(); err != nil {
		w.tx = nil
		return err
	}
	w.tx = nil
	tx, err := w.db.Begin(true)
	w.tx = tx
	w.dirty = 0
	return err
}
func (w *writer) finish() error {
	if w.tx != nil {
		err := w.tx.Commit()
		w.tx = nil
		if err != nil {
			return err
		}
	}
	if err := w.db.Sync(); err != nil {
		return err
	}
	err := w.db.Close()
	w.db = nil
	return err
}
func (w *writer) Close() error {
	var err error
	if w.tx != nil {
		err = w.tx.Rollback()
		w.tx = nil
	}
	if w.db != nil {
		err = errors.Join(err, w.db.Close())
		w.db = nil
	}
	return err
}
