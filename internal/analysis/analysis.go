// Package analysis attributes actual replayed MVCC work to storage keys and
// Kubernetes storage prefixes. Physical attribution is explicitly a model.
package analysis

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Config struct {
	Context   context.Context
	OutputDir string
	Window    time.Duration
	TopN      int
	LabelKeys []string
}

type Event struct {
	Kind                                           string
	Key                                            []byte
	Value                                          []byte `json:"-"` // Borrowed during Event; never retained in outputs.
	Revision, SubRevision, ValueBytes, RecordBytes int64
	Tombstone                                      bool
	Index                                          uint64
	VirtualNS                                      int64
}

type Commit struct {
	Number       int    `json:"number"`
	Index        uint64 `json:"index"`
	VirtualNS    int64  `json:"virtual_ns"`
	DBSize       int64  `json:"db_size"`
	DBSizeInUse  int64  `json:"db_size_in_use"`
	FreePages    int    `json:"free_pages"`
	PendingPages int    `json:"pending_pages"`
	Reason       string `json:"reason"`
}

// Stock counts exact retained MVCC record bytes, including revision bucket keys
// and serialized KV metadata. It excludes bbolt page headers and unused space.
type Stock struct {
	RetainedRecords int64 `json:"retained_records"`
	RetainedBytes   int64 `json:"retained_mvcc_bytes"`
	LiveObjects     int64 `json:"live_objects"`
	LiveRecordBytes int64 `json:"live_record_bytes"`
	LiveValueBytes  int64 `json:"live_value_bytes"`
	HistoryBytes    int64 `json:"history_bytes"`
}

type Metrics struct {
	Puts                             int64 `json:"puts"`
	Deletes                          int64 `json:"deletes"`
	ValueWriteBytes                  int64 `json:"value_write_bytes"`
	RecordWriteBytes                 int64 `json:"record_write_bytes"`
	SupersededLiveBytes              int64 `json:"superseded_live_record_bytes"`
	CompactedRecords                 int64 `json:"compacted_records"`
	CompactedBytes                   int64 `json:"compacted_mvcc_bytes"`
	EstimatedDBGrowthBytes           int64 `json:"estimated_db_growth_bytes"`
	EstimatedFreeGrowthBytes         int64 `json:"estimated_free_growth_bytes"`
	EstimatedDeferredFreeGrowthBytes int64 `json:"estimated_deferred_free_growth_bytes"`
	EstimatedFreeReuseBytes          int64 `json:"estimated_free_reuse_bytes"`
}

type Row struct {
	WindowStartNS      int64    `json:"window_start_ns"`
	WindowEndNS        int64    `json:"window_end_ns"`
	Identity           Identity `json:"identity"`
	Metadata           Metadata `json:"metadata"`
	Start              Stock    `json:"start"`
	End                Stock    `json:"end"`
	Metrics            Metrics  `json:"metrics"`
	RetainedDeltaBytes int64    `json:"retained_delta_bytes"`
	HistoryDeltaBytes  int64    `json:"history_delta_bytes"`
	FirstIndex         uint64   `json:"first_index,omitempty"`
	LastIndex          uint64   `json:"last_index,omitempty"`
}

type Rank struct {
	Identity Identity `json:"identity"`
	Bytes    int64    `json:"bytes"`
}

type Rankings struct {
	HistoryGrowthBytes  int64  `json:"history_growth_bytes"`
	RetainedGrowthBytes int64  `json:"retained_growth_bytes"`
	FreeNetGrowthBytes  int64  `json:"estimated_free_net_growth_bytes"`
	ValueWrites         []Rank `json:"value_writes"`
	HistoryGrowth       []Rank `json:"history_growth"`
	RetainedGrowth      []Rank `json:"retained_growth"`
	Compaction          []Rank `json:"compacted_mvcc_bytes"`
	DBGrowth            []Rank `json:"estimated_db_growth"`
	FreeGrowth          []Rank `json:"estimated_free_growth"`
	FreeNetGrowth       []Rank `json:"estimated_free_net_growth"`
}

type Window struct {
	StartNS                     int64    `json:"start_ns"`
	EndNS                       int64    `json:"end_ns"`
	Start                       Stock    `json:"start"`
	End                         Stock    `json:"end"`
	Metrics                     Metrics  `json:"metrics"`
	PhysicalStart               Commit   `json:"physical_start"`
	PhysicalEnd                 Commit   `json:"physical_end"`
	CommitCount                 int64    `json:"commit_count"`
	DBGrowthBytes               int64    `json:"db_growth_bytes"`
	DBShrinkBytes               int64    `json:"db_shrink_bytes"`
	FreeGrowthBytes             int64    `json:"free_growth_bytes"`
	FreeReuseBytes              int64    `json:"free_reuse_bytes"`
	UnattributedDBGrowthBytes   int64    `json:"unattributed_db_growth_bytes"`
	UnattributedFreeGrowthBytes int64    `json:"unattributed_free_growth_bytes"`
	UnattributedFreeReuseBytes  int64    `json:"unattributed_free_reuse_bytes"`
	Objects                     Rankings `json:"top_objects"`
	Resources                   Rankings `json:"top_resources"`
}

type Summary struct {
	Version                     string            `json:"version"`
	Model                       string            `json:"model"`
	WindowNS                    int64             `json:"window_ns"`
	TopN                        int               `json:"top_n"`
	WindowCount                 int64             `json:"window_count"`
	ObjectRows                  int64             `json:"object_rows"`
	ResourceRows                int64             `json:"resource_rows"`
	BaselineObjects             int64             `json:"baseline_objects"`
	FinalObjects                int64             `json:"final_objects"`
	Initial                     Stock             `json:"initial"`
	Final                       Stock             `json:"final"`
	Metrics                     Metrics           `json:"metrics"`
	DBGrowthBytes               int64             `json:"db_growth_bytes"`
	DBShrinkBytes               int64             `json:"db_shrink_bytes"`
	FreeGrowthBytes             int64             `json:"free_growth_bytes"`
	FreeReuseBytes              int64             `json:"free_reuse_bytes"`
	UnattributedDBGrowthBytes   int64             `json:"unattributed_db_growth_bytes"`
	UnattributedFreeGrowthBytes int64             `json:"unattributed_free_growth_bytes"`
	UnattributedFreeReuseBytes  int64             `json:"unattributed_free_reuse_bytes"`
	Files                       map[string]string `json:"files"`
	SHA256                      map[string]string `json:"sha256"`
	LabelKeys                   []string          `json:"label_keys,omitempty"`
}

type objectState struct {
	Identity    Identity `json:"identity"`
	Metadata    Metadata `json:"metadata"`
	Stock       Stock    `json:"stock"`
	Revision    int64    `json:"revision"`
	SubRevision int64    `json:"sub_revision"`
}
type pending struct {
	Written, Removed int64
	First, Last      uint64
}

var objects = []byte("objects")
var resources = []byte("resources")
var objectWindow = []byte("object_window")
var resourceWindow = []byte("resource_window")
var pendingBucket = []byte("pending")
var releaseBucket = []byte("previous_compaction_release")

type Analyzer struct {
	cfg                       Config
	db                        *bolt.DB
	tx                        *bolt.Tx
	outputs                   map[string]*output
	stock                     Stock
	window                    Window
	summary                   Summary
	physical                  Commit
	lastTime                  int64
	baselineEvents, dirty     int
	started, finished, closed bool
}

func New(cfg Config) (_ *Analyzer, err error) {
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if err = cfg.Context.Err(); err != nil {
		return nil, err
	}
	if cfg.Window == 0 {
		cfg.Window = time.Minute
	}
	if cfg.TopN == 0 {
		cfg.TopN = 10
	}
	if cfg.Window <= 0 || cfg.TopN < 1 || cfg.TopN > 100 {
		return nil, fmt.Errorf("analysis window must be positive and top N between 1 and 100")
	}
	cfg.LabelKeys, err = NormalizeLabelKeys(cfg.LabelKeys)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(cfg.OutputDir, 0700); err != nil {
		return nil, err
	}
	a := &Analyzer{cfg: cfg, outputs: make(map[string]*output)}
	defer func() {
		if err != nil {
			a.Close()
		}
	}()
	a.db, err = bolt.Open(filepath.Join(cfg.OutputDir, "analysis.db"), 0600, &bolt.Options{Timeout: time.Second, NoSync: true})
	if err != nil {
		return nil, err
	}
	a.tx, err = a.db.Begin(true)
	if err != nil {
		return nil, err
	}
	for _, name := range [][]byte{objects, resources, objectWindow, resourceWindow, pendingBucket, releaseBucket} {
		if _, err = a.tx.CreateBucket(name); err != nil {
			return nil, err
		}
	}
	a.summary = Summary{Version: "object-analysis-v3", Model: "Exact MVCC record accounting; DB growth/free reuse apportioned by current changed record bytes. Free growth uses only previous-commit compaction weights when that commit reports pending pages, reflecting bbolt's delayed free-page publication. Release weights expire after one commit; current compaction cannot claim an older release. Estimates are associations, not unique page ownership. Unmatched changes remain unattributed. Sparse rows carry unchanged stocks forward. Kubernetes metadata is the latest observed value at each object-window endpoint; all work in that window uses that metadata, including previously retained records. Managed-field managers are ownership hints, not authenticated writers.", WindowNS: int64(cfg.Window), TopN: cfg.TopN, Files: make(map[string]string), SHA256: make(map[string]string), LabelKeys: cfg.LabelKeys}
	for _, name := range []string{"baseline.objects", "baseline.resources", "objects", "resources", "windows", "final.objects", "final.resources"} {
		a.summary.Files[name] = name + ".jsonl"
		var stream *output
		stream, err = newOutput(filepath.Join(cfg.OutputDir, name+".jsonl"))
		if err != nil {
			return nil, err
		}
		a.outputs[name] = stream
	}
	a.summary.Files["state"] = "analysis.db"
	return a, nil
}

func (a *Analyzer) Event(e Event) error {
	if err := a.cfg.Context.Err(); err != nil {
		return err
	}
	if a.closed || a.finished {
		return fmt.Errorf("analysis is closed")
	}
	if e.RecordBytes <= 0 || e.ValueBytes < 0 || e.Revision < 0 || e.SubRevision < 0 || len(e.Key) == 0 {
		return fmt.Errorf("invalid MVCC observation")
	}
	baseline := e.Kind == "baseline"
	if baseline == a.started {
		return fmt.Errorf("baseline observations must precede Start; mutations must follow Start")
	}
	if !baseline {
		if err := a.advance(e.VirtualNS); err != nil {
			return err
		}
	}
	var state objectState
	if err := get(a.tx.Bucket(objects), e.Key, &state); err != nil {
		return err
	}
	if state.Identity.Key == "" {
		state.Identity = Identify(e.Key)
		state.Metadata.Status = "unavailable"
	}
	before := state.Stock
	var delta Metrics
	switch e.Kind {
	case "baseline", "put", "delete":
		if state.Stock.RetainedRecords > 0 && (e.Revision < state.Revision || e.Revision == state.Revision && e.SubRevision <= state.SubRevision) {
			return fmt.Errorf("nonmonotonic object revision for %q", e.Key)
		}
		if e.Kind != "delete" && !e.Tombstone {
			state.Metadata = DecodeMetadata(e.Value, a.cfg.LabelKeys)
			state.Metadata.Revision, state.Metadata.VirtualNS = e.Revision, e.VirtualNS
			if state.Metadata.Status == "decoded-json" || state.Metadata.Status == "decoded-protobuf" {
				id := state.Identity
				state.Metadata.IdentityMismatch = id.Name != "" && id.Name != state.Metadata.Name || id.Scope == "namespaced" && id.Namespace != state.Metadata.Namespace || id.Scope == "cluster" && state.Metadata.Namespace != ""
			}
		}
		state.Stock.RetainedBytes += e.RecordBytes
		state.Stock.RetainedRecords++
		state.Stock.LiveObjects = 1
		state.Stock.LiveRecordBytes = e.RecordBytes
		state.Stock.LiveValueBytes = e.ValueBytes
		if e.Tombstone || e.Kind == "delete" {
			state.Stock.LiveObjects = 0
			state.Stock.LiveRecordBytes = 0
			state.Stock.LiveValueBytes = 0
		}
		state.Revision, state.SubRevision = e.Revision, e.SubRevision
		if !baseline {
			delta.RecordWriteBytes = e.RecordBytes
			delta.SupersededLiveBytes = before.LiveRecordBytes
			if e.Kind == "put" {
				delta.Puts = 1
				delta.ValueWriteBytes = e.ValueBytes
			} else {
				delta.Deletes = 1
			}
		}
	case "compact":
		state.Stock.RetainedRecords--
		state.Stock.RetainedBytes -= e.RecordBytes
		if e.Revision == state.Revision && e.SubRevision == state.SubRevision {
			if state.Stock.LiveObjects != 0 {
				return fmt.Errorf("compaction removed live latest version for %q", e.Key)
			}
			state.Revision, state.SubRevision = 0, 0
		}
		delta.CompactedRecords = 1
		delta.CompactedBytes = e.RecordBytes
	default:
		return fmt.Errorf("unsupported analysis event %q", e.Kind)
	}
	state.Stock.HistoryBytes = state.Stock.RetainedBytes - state.Stock.LiveRecordBytes
	if state.Stock.RetainedBytes < 0 || state.Stock.RetainedRecords < 0 || state.Stock.HistoryBytes < 0 {
		return fmt.Errorf("negative retained stock for %q", e.Key)
	}
	if err := put(a.tx.Bucket(objects), e.Key, state); err != nil {
		return err
	}
	resourceKey, resourceIdentity := resource(state.Identity)
	var group objectState
	if err := get(a.tx.Bucket(resources), resourceKey, &group); err != nil {
		return err
	}
	group.Identity = resourceIdentity
	groupBefore := group.Stock
	change := subtract(state.Stock, before)
	addStock(&group.Stock, change)
	addStock(&a.stock, change)
	if err := put(a.tx.Bucket(resources), resourceKey, group); err != nil {
		return err
	}
	if baseline {
		a.baselineEvents++
	} else {
		if err := a.updateRow(objectWindow, e.Key, state.Identity, before, state.Stock, delta, e.Index, e.Index); err != nil {
			return err
		}
		if err := a.updateRow(resourceWindow, resourceKey, group.Identity, groupBefore, group.Stock, delta, e.Index, e.Index); err != nil {
			return err
		}
		addMetrics(&a.window.Metrics, delta)
		addMetrics(&a.summary.Metrics, delta)
		var p pending
		if err := get(a.tx.Bucket(pendingBucket), e.Key, &p); err != nil {
			return err
		}
		p.Written += delta.RecordWriteBytes
		p.Removed += delta.CompactedBytes
		indices(&p.First, &p.Last, e.Index, e.Index)
		if err := put(a.tx.Bucket(pendingBucket), e.Key, p); err != nil {
			return err
		}
	}
	a.dirty++
	if a.dirty >= 4096 {
		return a.checkpoint()
	}
	return nil
}

func (a *Analyzer) Start(initial Commit) error {
	if err := a.cfg.Context.Err(); err != nil {
		return err
	}
	if a.started || a.finished || a.closed {
		return fmt.Errorf("analysis already started or closed")
	}
	if initial.VirtualNS != 0 || initial.DBSize < initial.DBSizeInUse || initial.DBSizeInUse < 0 {
		return fmt.Errorf("invalid initial physical state")
	}
	a.started = true
	a.physical = initial
	a.window = Window{StartNS: 0, Start: a.stock, PhysicalStart: initial, PhysicalEnd: initial}
	a.summary.Initial = a.stock
	var err error
	a.summary.BaselineObjects, err = a.inventory(objects, "baseline.objects")
	if err != nil {
		return err
	}
	if _, err = a.inventory(resources, "baseline.resources"); err != nil {
		return err
	}
	return a.checkpoint()
}

func (a *Analyzer) Commit(c Commit) error {
	if !a.started || a.finished || a.closed {
		return fmt.Errorf("analysis not running")
	}
	if c.DBSize < c.DBSizeInUse || c.DBSizeInUse < 0 {
		return fmt.Errorf("invalid commit physical state")
	}
	if err := a.advance(c.VirtualNS); err != nil {
		return err
	}
	dbDelta := c.DBSize - a.physical.DBSize
	freeDelta := (c.DBSize - c.DBSizeInUse) - (a.physical.DBSize - a.physical.DBSizeInUse)
	growth, shrink, free, reuse := positive(dbDelta), positive(-dbDelta), positive(freeDelta), positive(-freeDelta)
	a.window.DBGrowthBytes += growth
	a.summary.DBGrowthBytes += growth
	a.window.DBShrinkBytes += shrink
	a.summary.DBShrinkBytes += shrink
	a.window.FreeGrowthBytes += free
	a.summary.FreeGrowthBytes += free
	a.window.FreeReuseBytes += reuse
	a.summary.FreeReuseBytes += reuse
	var changed int64
	if err := a.tx.Bucket(pendingBucket).ForEach(func(k, v []byte) error {
		if err := a.cfg.Context.Err(); err != nil {
			return err
		}
		var p pending
		if err := json.Unmarshal(v, &p); err != nil {
			return err
		}
		changed += p.Written + p.Removed
		return nil
	}); err != nil {
		return err
	}
	if changed == 0 {
		a.window.UnattributedDBGrowthBytes += growth
		a.summary.UnattributedDBGrowthBytes += growth
		a.window.UnattributedFreeReuseBytes += reuse
		a.summary.UnattributedFreeReuseBytes += reuse
	}
	// In the pinned bbolt backend, stats at commit N report the release of
	// pending pages from N-1, net of N's allocations. Keep exactly one commit
	// of compaction associations; never carry stale weights indefinitely.
	var releaseRemoved int64
	if a.physical.PendingPages > 0 {
		if err := a.tx.Bucket(releaseBucket).ForEach(func(k, v []byte) error {
			if err := a.cfg.Context.Err(); err != nil {
				return err
			}
			var p pending
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			releaseRemoved += p.Removed
			return nil
		}); err != nil {
			return err
		}
	}
	freeTotal := releaseRemoved
	if freeTotal == 0 {
		a.window.UnattributedFreeGrowthBytes += free
		a.summary.UnattributedFreeGrowthBytes += free
	}
	var consumed, assignedGrowth, assignedReuse int64
	err := a.tx.Bucket(pendingBucket).ForEach(func(k, v []byte) error {
		if err := a.cfg.Context.Err(); err != nil {
			return err
		}
		var p pending
		if err := json.Unmarshal(v, &p); err != nil {
			return err
		}
		consumed += p.Written + p.Removed
		g, u := share(growth, consumed, changed), share(reuse, consumed, changed)
		delta := Metrics{EstimatedDBGrowthBytes: g - assignedGrowth, EstimatedFreeReuseBytes: u - assignedReuse}
		assignedGrowth, assignedReuse = g, u
		return a.attribute(k, p, delta)
	})
	if err != nil {
		return err
	}
	if free > 0 && freeTotal > 0 {
		var consumed, assigned int64
		if err = a.tx.Bucket(releaseBucket).ForEach(func(k, v []byte) error {
			if err := a.cfg.Context.Err(); err != nil {
				return err
			}
			var p pending
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			consumed += p.Removed
			n := share(free, consumed, freeTotal)
			delta := Metrics{EstimatedFreeGrowthBytes: n - assigned}
			assigned = n
			delta.EstimatedDeferredFreeGrowthBytes = delta.EstimatedFreeGrowthBytes
			return a.attribute(k, p, delta)
		}); err != nil {
			return err
		}
	}
	if err = a.reset(releaseBucket); err != nil {
		return err
	}
	if c.PendingPages > 0 {
		if err = a.tx.Bucket(pendingBucket).ForEach(func(k, v []byte) error {
			if err := a.cfg.Context.Err(); err != nil {
				return err
			}
			var p pending
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			if p.Removed == 0 {
				return nil
			}
			p.Written = 0
			return put(a.tx.Bucket(releaseBucket), k, p)
		}); err != nil {
			return err
		}
	}
	if err = a.reset(pendingBucket); err != nil {
		return err
	}
	a.physical = c
	a.window.PhysicalEnd = c
	a.window.CommitCount++
	return a.checkpoint()
}

func (a *Analyzer) attribute(key []byte, p pending, delta Metrics) error {
	if delta.EstimatedDBGrowthBytes == 0 && delta.EstimatedFreeGrowthBytes == 0 && delta.EstimatedFreeReuseBytes == 0 {
		return nil
	}
	var state objectState
	if err := get(a.tx.Bucket(objects), key, &state); err != nil {
		return err
	}
	if err := a.updateRow(objectWindow, key, state.Identity, state.Stock, state.Stock, delta, p.First, p.Last); err != nil {
		return err
	}
	rk, ri := resource(state.Identity)
	var rs objectState
	if err := get(a.tx.Bucket(resources), rk, &rs); err != nil {
		return err
	}
	if err := a.updateRow(resourceWindow, rk, ri, rs.Stock, rs.Stock, delta, p.First, p.Last); err != nil {
		return err
	}
	addMetrics(&a.window.Metrics, delta)
	addMetrics(&a.summary.Metrics, delta)
	return nil
}

func (a *Analyzer) Finish(endVirtualNS int64) (Summary, error) {
	if !a.started || a.finished || a.closed {
		return a.summary, fmt.Errorf("analysis not running")
	}
	if err := a.advance(endVirtualNS); err != nil {
		return a.summary, err
	}
	if k, _ := a.tx.Bucket(pendingBucket).Cursor().First(); k != nil {
		return a.summary, fmt.Errorf("uncommitted MVCC observations at analysis end")
	}
	if err := a.flushWindow(endVirtualNS); err != nil {
		return a.summary, err
	}
	var err error
	a.summary.Final = a.stock
	a.summary.FinalObjects, err = a.inventory(objects, "final.objects")
	if err != nil {
		return a.summary, err
	}
	if _, err = a.inventory(resources, "final.resources"); err != nil {
		return a.summary, err
	}
	if err = a.tx.Commit(); err != nil {
		return a.summary, err
	}
	a.tx = nil
	if err = a.db.Sync(); err != nil {
		return a.summary, err
	}
	for name, o := range a.outputs {
		if err = o.Close(); err != nil {
			return a.summary, err
		}
		a.summary.SHA256[name] = o.Hash()
	}
	data, err := json.MarshalIndent(a.summary, "", "  ")
	if err != nil {
		return a.summary, err
	}
	if err = os.WriteFile(filepath.Join(a.cfg.OutputDir, "summary.json"), append(data, '\n'), 0600); err != nil {
		return a.summary, err
	}
	a.finished = true
	return a.summary, nil
}

func (a *Analyzer) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	var errs []error
	if a.tx != nil {
		errs = append(errs, a.tx.Rollback())
		a.tx = nil
	}
	for _, o := range a.outputs {
		errs = append(errs, o.Close())
	}
	if a.db != nil {
		errs = append(errs, a.db.Close())
	}
	return errors.Join(errs...)
}

func (a *Analyzer) advance(t int64) error {
	if err := a.cfg.Context.Err(); err != nil {
		return err
	}
	if t < a.lastTime || t < 0 {
		return fmt.Errorf("nonmonotonic analysis time")
	}
	for t >= a.window.StartNS+int64(a.cfg.Window) {
		end := a.window.StartNS + int64(a.cfg.Window)
		if err := a.flushWindow(end); err != nil {
			return err
		}
		a.window = Window{StartNS: end, Start: a.stock, PhysicalStart: a.physical, PhysicalEnd: a.physical}
	}
	a.lastTime = t
	return nil
}

func (a *Analyzer) updateRow(bucket, key []byte, identity Identity, before, after Stock, delta Metrics, first, last uint64) error {
	var row Row
	b := a.tx.Bucket(bucket)
	if raw := b.Get(key); raw != nil {
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
	} else {
		row = Row{WindowStartNS: a.window.StartNS, Identity: identity, Start: before}
	}
	row.End = after
	if string(bucket) == string(objectWindow) {
		var state objectState
		if err := get(a.tx.Bucket(objects), key, &state); err != nil {
			return err
		}
		row.Metadata = state.Metadata
	}
	addMetrics(&row.Metrics, delta)
	indices(&row.FirstIndex, &row.LastIndex, first, last)
	return put(b, key, row)
}

func (a *Analyzer) flushWindow(end int64) error {
	if err := a.cfg.Context.Err(); err != nil {
		return err
	}
	a.window.EndNS = end
	a.window.End = a.stock
	a.window.PhysicalEnd = a.physical
	for _, item := range []struct {
		bucket []byte
		name   string
		rank   *Rankings
		count  *int64
	}{{objectWindow, "objects", &a.window.Objects, &a.summary.ObjectRows}, {resourceWindow, "resources", &a.window.Resources, &a.summary.ResourceRows}} {
		if err := a.tx.Bucket(item.bucket).ForEach(func(k, v []byte) error {
			if err := a.cfg.Context.Err(); err != nil {
				return err
			}
			var row Row
			if err := json.Unmarshal(v, &row); err != nil {
				return err
			}
			row.WindowEndNS = end
			row.RetainedDeltaBytes = row.End.RetainedBytes - row.Start.RetainedBytes
			row.HistoryDeltaBytes = row.End.HistoryBytes - row.Start.HistoryBytes
			addRankings(item.rank, row, a.cfg.TopN)
			if err := a.outputs[item.name].Write(row); err != nil {
				return err
			}
			*item.count++
			return nil
		}); err != nil {
			return err
		}
		if err := a.reset(item.bucket); err != nil {
			return err
		}
	}
	if err := a.outputs["windows"].Write(a.window); err != nil {
		return err
	}
	a.summary.WindowCount++
	return a.checkpoint()
}

func (a *Analyzer) inventory(bucket []byte, name string) (int64, error) {
	var count int64
	err := a.tx.Bucket(bucket).ForEach(func(k, v []byte) error {
		if err := a.cfg.Context.Err(); err != nil {
			return err
		}
		var state objectState
		if err := json.Unmarshal(v, &state); err != nil {
			return err
		}
		if state.Stock.RetainedRecords == 0 {
			return nil
		}
		count++
		return a.outputs[name].Write(struct {
			Identity Identity `json:"identity"`
			Metadata Metadata `json:"metadata"`
			Stock    Stock    `json:"stock"`
		}{state.Identity, state.Metadata, state.Stock})
	})
	return count, err
}

func (a *Analyzer) reset(bucket []byte) error {
	if err := a.tx.DeleteBucket(bucket); err != nil {
		return err
	}
	_, err := a.tx.CreateBucket(bucket)
	return err
}
func (a *Analyzer) checkpoint() error {
	if err := a.tx.Commit(); err != nil {
		return err
	}
	a.tx = nil
	tx, err := a.db.Begin(true)
	a.tx = tx
	a.dirty = 0
	return err
}
func get(b *bolt.Bucket, key []byte, out any) error {
	if data := b.Get(key); data != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
func put(b *bolt.Bucket, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}
func resource(id Identity) ([]byte, Identity) {
	r := Identity{Resource: id.Resource, APIGroup: id.APIGroup, Source: "storage-prefix", Confidence: "storage-prefix"}
	key, _ := json.Marshal([]string{id.Resource, id.APIGroup})
	return key, r
}
func positive(n int64) int64 {
	if n > 0 {
		return n
	}
	return 0
}
func share(amount, cumulative, total int64) int64 {
	if amount == 0 || total == 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(amount), uint64(cumulative))
	q, _ := bits.Div64(hi, lo, uint64(total))
	return int64(q)
}
func indices(first, last *uint64, a, b uint64) {
	if a != 0 && (*first == 0 || a < *first) {
		*first = a
	}
	if b > *last {
		*last = b
	}
}
func subtract(a, b Stock) Stock {
	return Stock{a.RetainedRecords - b.RetainedRecords, a.RetainedBytes - b.RetainedBytes, a.LiveObjects - b.LiveObjects, a.LiveRecordBytes - b.LiveRecordBytes, a.LiveValueBytes - b.LiveValueBytes, a.HistoryBytes - b.HistoryBytes}
}
func addStock(a *Stock, b Stock) {
	a.RetainedRecords += b.RetainedRecords
	a.RetainedBytes += b.RetainedBytes
	a.LiveObjects += b.LiveObjects
	a.LiveRecordBytes += b.LiveRecordBytes
	a.LiveValueBytes += b.LiveValueBytes
	a.HistoryBytes += b.HistoryBytes
}
func addMetrics(a *Metrics, b Metrics) {
	a.Puts += b.Puts
	a.Deletes += b.Deletes
	a.ValueWriteBytes += b.ValueWriteBytes
	a.RecordWriteBytes += b.RecordWriteBytes
	a.SupersededLiveBytes += b.SupersededLiveBytes
	a.CompactedRecords += b.CompactedRecords
	a.CompactedBytes += b.CompactedBytes
	a.EstimatedDBGrowthBytes += b.EstimatedDBGrowthBytes
	a.EstimatedFreeGrowthBytes += b.EstimatedFreeGrowthBytes
	a.EstimatedDeferredFreeGrowthBytes += b.EstimatedDeferredFreeGrowthBytes
	a.EstimatedFreeReuseBytes += b.EstimatedFreeReuseBytes
}
func addRankings(r *Rankings, row Row, n int) {
	r.HistoryGrowthBytes += positive(row.HistoryDeltaBytes)
	r.RetainedGrowthBytes += positive(row.RetainedDeltaBytes)
	freeNet := row.Metrics.EstimatedFreeGrowthBytes - row.Metrics.EstimatedFreeReuseBytes
	r.FreeNetGrowthBytes += positive(freeNet)
	addRank(&r.ValueWrites, row.Identity, row.Metrics.ValueWriteBytes, n)
	addRank(&r.HistoryGrowth, row.Identity, row.HistoryDeltaBytes, n)
	addRank(&r.RetainedGrowth, row.Identity, row.RetainedDeltaBytes, n)
	addRank(&r.Compaction, row.Identity, row.Metrics.CompactedBytes, n)
	addRank(&r.DBGrowth, row.Identity, row.Metrics.EstimatedDBGrowthBytes, n)
	addRank(&r.FreeGrowth, row.Identity, row.Metrics.EstimatedFreeGrowthBytes, n)
	addRank(&r.FreeNetGrowth, row.Identity, freeNet, n)
}
func addRank(list *[]Rank, id Identity, value int64, n int) {
	if value <= 0 {
		return
	}
	*list = append(*list, Rank{id, value})
	sort.Slice(*list, func(i, j int) bool {
		if (*list)[i].Bytes != (*list)[j].Bytes {
			return (*list)[i].Bytes > (*list)[j].Bytes
		}
		a, _ := json.Marshal((*list)[i].Identity)
		b, _ := json.Marshal((*list)[j].Identity)
		return string(a) < string(b)
	})
	if len(*list) > n {
		*list = (*list)[:n]
	}
}

type output struct {
	file    *os.File
	writer  *bufio.Writer
	encoder *json.Encoder
	hash    hash.Hash
	closed  bool
}

func newOutput(path string) (*output, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(f, h), 64<<10)
	return &output{file: f, writer: w, encoder: json.NewEncoder(w), hash: h}, nil
}
func (o *output) Write(v any) error { return o.encoder.Encode(v) }
func (o *output) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	return errors.Join(o.writer.Flush(), o.file.Sync(), o.file.Close())
}
func (o *output) Hash() string { return hex.EncodeToString(o.hash.Sum(nil)) }
