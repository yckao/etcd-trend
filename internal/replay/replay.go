// Package replay executes captured committed Raft entries against the real etcd
// MVCC and bbolt backend in an isolated, explicitly scheduled runtime.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/third_party/etcd-server/auth"
	"github.com/yckao/etcd-trend/third_party/etcd-server/etcdserver/api/membership"
	"github.com/yckao/etcd-trend/third_party/etcd-server/etcdserver/api/v3alarm"
	"github.com/yckao/etcd-trend/third_party/etcd-server/etcdserver/apply"
	"github.com/yckao/etcd-trend/third_party/etcd-server/etcdserver/cindex"
	"github.com/yckao/etcd-trend/third_party/etcd-server/lease"
	storage "github.com/yckao/etcd-trend/third_party/etcd-server/storage"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/backend"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/mvcc"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/schema"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const EngineVersion = "etcd-3.6.0-replay-v3"

// Config requires canonical, committed, contiguous entries after the baseline.
// EntryTimes uses elapsed source/observation time, not wall-clock replay time.
type Config struct {
	BaselinePath            string
	SourceVersion           string
	Source                  EntrySource
	RetainObservations      bool // Test/debug only; retains all observations and final KVs.
	Entries                 []raftpb.Entry
	EntryTimes              map[uint64]time.Duration
	OutputDir               string
	Speed                   float64 // 0 is unpaced maximum speed; positive values pace virtual time.
	BatchInterval           time.Duration
	BatchLimit              int
	CompactionBatchLimit    int
	CompactionSleepInterval time.Duration
	QuotaBackendBytes       int64
	MemberID                uint64
	ClusterID               uint64
	AnalyzeObjects          bool
	AnalysisWindow          time.Duration
	AnalysisTopN            int
	AnalysisLabelKeys       []string
	OnCommit                func(Commit) // Optional streaming observer; must not access the backend.
}

type Commit struct {
	Number       int    `json:"number"`
	TxID         int    `json:"txid"`
	Index        uint64 `json:"index"`
	VirtualNS    int64  `json:"virtual_ns"`
	Reason       string `json:"reason"`
	DBSize       int64  `json:"db_size"`
	DBSizeInUse  int64  `json:"db_size_in_use"`
	FreePages    int    `json:"free_pages"`
	PendingPages int    `json:"pending_pages"`
}

type Operation struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	VirtualNS int64  `json:"virtual_ns"`
	Type      string `json:"type"`
	Revision  int64  `json:"revision"`
	Error     string `json:"error,omitempty"`
}

type Result struct {
	Engine               string            `json:"engine"`
	SourceVersion        string            `json:"source_version"`
	StorageEngineVersion string            `json:"storage_engine_version"`
	Compatibility        string            `json:"compatibility"`
	CommitCount          int               `json:"commit_count"`
	OperationCount       int               `json:"operation_count"`
	OperationErrorCount  int               `json:"operation_error_count"`
	CommitsFile          string            `json:"commits_file"`
	OperationsFile       string            `json:"operations_file"`
	CommitsSHA256        string            `json:"commits_sha256"`
	OperationsSHA256     string            `json:"operations_sha256"`
	Model                string            `json:"model"`
	Baseline             etcdutil.DBInfo   `json:"baseline"`
	AppliedEntries       int               `json:"applied_entries"`
	FinalIndex           uint64            `json:"final_index"`
	FinalRevision        int64             `json:"final_revision"`
	LogicalSHA256        string            `json:"logical_sha256"`
	KeyCount             int               `json:"key_count"`
	WallDurationNS       int64             `json:"wall_duration_ns"`
	VirtualDurationNS    int64             `json:"virtual_duration_ns"`
	Speed                float64           `json:"speed"`
	BatchIntervalNS      int64             `json:"batch_interval_ns"`
	BatchLimit           int               `json:"batch_limit"`
	CompactionBatchLimit int               `json:"compaction_batch_limit"`
	CompactionSleepNS    int64             `json:"compaction_sleep_ns"`
	Commits              []Commit          `json:"-"`
	Operations           []Operation       `json:"-"`
	KVs                  []mvccpb.KeyValue `json:"-"`
	AnalysisDirectory    string            `json:"analysis_directory,omitempty"`
	Analysis             *analysis.Summary `json:"analysis,omitempty"`
}

type runner struct {
	ctx             context.Context
	cfg             Config
	result          Result
	be              backend.Backend
	kv              mvcc.KV
	ci              cindex.ConsistentIndexer
	now, timeTick   time.Duration
	started         time.Time
	index, term     uint64
	reason          string
	observe         bool
	scheduleErr     error
	observationErr  error
	commits         *observationStream
	operations      *observationStream
	objects         *analysis.Analyzer
	analysisStarted bool
	analysisCommits int
}

// Run creates a new output directory and never edits the baseline or WAL input.
// Physical compaction is modeled as a serial barrier. Its batch sleeps advance
// virtual time, allowing commit ticks, but future WAL entries do not interleave.
func Run(ctx context.Context, cfg Config) (res Result, err error) {
	if cfg.OutputDir == "" || cfg.BaselinePath == "" {
		return res, fmt.Errorf("baseline and output directory are required")
	}
	if cfg.SourceVersion == "" {
		cfg.SourceVersion = "3.6.0"
	}
	sourceVersion, err := etcdutil.ParseSourceVersion(cfg.SourceVersion)
	if err != nil {
		return res, err
	}
	cfg.SourceVersion = sourceVersion.Version
	if cfg.Source != nil && (len(cfg.Entries) != 0 || len(cfg.EntryTimes) != 0) {
		return res, fmt.Errorf("use either streaming source or slice entries, not both")
	}
	if cfg.Speed < 0 || math.IsNaN(cfg.Speed) || math.IsInf(cfg.Speed, 0) {
		return res, fmt.Errorf("speed must be finite and nonnegative")
	}
	if cfg.BatchInterval == 0 {
		cfg.BatchInterval = 100 * time.Millisecond
	}
	if cfg.BatchLimit == 0 {
		cfg.BatchLimit = 10000
	}
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = 1000
	}
	if cfg.CompactionSleepInterval == 0 {
		cfg.CompactionSleepInterval = 10 * time.Millisecond
	}
	if cfg.QuotaBackendBytes == 0 {
		cfg.QuotaBackendBytes = 8 * 1024 * 1024 * 1024
	}
	if cfg.BatchInterval < 0 || cfg.BatchLimit < 0 || cfg.CompactionBatchLimit < 0 || cfg.CompactionSleepInterval < 0 {
		return res, fmt.Errorf("invalid scheduler configuration")
	}
	base, err := etcdutil.InspectDB(cfg.BaselinePath)
	if err != nil {
		return res, fmt.Errorf("baseline: %w", err)
	}
	if cfg.Source == nil {
		expected := base.Index + 1
		var lastTime time.Duration
		for _, e := range cfg.Entries {
			if e.Index != expected {
				return res, fmt.Errorf("entry gap: expected %d, got %d", expected, e.Index)
			}
			t, ok := cfg.EntryTimes[e.Index]
			if !ok {
				return res, fmt.Errorf("missing time anchor for entry %d", e.Index)
			}
			if t < lastTime || t < 0 {
				return res, fmt.Errorf("nonmonotonic entry time at %d", e.Index)
			}
			lastTime = t
			expected++
		}
		cfg.Source = &sliceSource{entries: cfg.Entries, times: cfg.EntryTimes}
	}
	if err = os.Mkdir(cfg.OutputDir, 0700); err != nil {
		return res, fmt.Errorf("create new replay directory: %w", err)
	}
	dbPath := filepath.Join(cfg.OutputDir, "replay.db")
	if err = copyDB(cfg.BaselinePath, dbPath); err != nil {
		return res, err
	}
	r := &runner{ctx: ctx, cfg: cfg, now: 0, timeTick: cfg.BatchInterval, started: time.Now(), index: base.Index, term: base.Term, reason: "startup"}
	r.result = Result{Engine: EngineVersion, SourceVersion: cfg.SourceVersion, StorageEngineVersion: "3.6.0", Compatibility: "source-version request adapters; fixed etcd 3.6 storage simulation, not original source-minor physical behavior", CommitsFile: "commits.jsonl", OperationsFile: "operations.jsonl", Model: "virtual timer; serial physical compaction; no autonomous lease expiry", Baseline: base, Speed: cfg.Speed, BatchIntervalNS: int64(cfg.BatchInterval), BatchLimit: cfg.BatchLimit, CompactionBatchLimit: cfg.CompactionBatchLimit, CompactionSleepNS: int64(cfg.CompactionSleepInterval)}
	if cfg.AnalyzeObjects {
		r.objects, err = analysis.New(analysis.Config{Context: ctx, OutputDir: filepath.Join(cfg.OutputDir, "analysis"), Window: cfg.AnalysisWindow, TopN: cfg.AnalysisTopN, LabelKeys: cfg.AnalysisLabelKeys})
		if err != nil {
			return r.result, err
		}
		defer r.objects.Close()
		if err = mvcc.ReplayVisitRecords(ctx, dbPath, func(e mvcc.ReplayMutation) error {
			return r.objects.Event(r.analysisEvent(e))
		}); err != nil {
			return r.result, fmt.Errorf("analyze baseline: %w", err)
		}
	}
	r.commits, err = newObservationStream(filepath.Join(cfg.OutputDir, "commits.jsonl"))
	if err != nil {
		return r.result, err
	}
	defer r.commits.Close()
	r.operations, err = newObservationStream(filepath.Join(cfg.OutputDir, "operations.jsonl"))
	if err != nil {
		return r.result, err
	}
	defer r.operations.Close()
	lg := zap.NewNop()
	ci := cindex.NewConsistentIndex(nil)
	r.ci = ci
	bcfg := backend.DefaultBackendConfig(lg)
	bcfg.Path = dbPath
	bcfg.BatchInterval = cfg.BatchInterval
	bcfg.BatchLimit = cfg.BatchLimit
	bcfg.ReplayManualCommit = true
	bcfg.Hooks = storage.NewBackendHooks(lg, ci)
	bcfg.OnReplayCommit = r.observeCommit
	r.be = backend.New(bcfg)
	if r.objects != nil {
		s := backend.ReplayStats(r.be)
		if err = r.objects.Start(analysis.Commit{Index: base.Index, DBSize: s.DBSize, DBSizeInUse: s.DBSizeInUse, FreePages: s.FreePages, PendingPages: s.PendingPages, Reason: "baseline"}); err != nil {
			_ = r.be.Close()
			return r.result, err
		}
		r.analysisStarted = true
	}
	ci.SetBackend(r.be)
	ci.ConsistentIndex()
	r.be.SetTxPostLockInsideApplyHook(func() { i, t := ci.ConsistentApplyingIndex(); ci.SetConsistentIndex(i, t) })
	defer func() {
		closeErr := r.be.Close()
		if err == nil {
			err = closeErr
		}
	}()
	cluster := membership.NewCluster(lg)
	cluster.SetBackend(schema.NewMembershipBackend(lg, r.be))
	cluster.SetID(types.ID(cfg.MemberID), types.ID(cfg.ClusterID))
	cluster.RecoverForReplay(func(*zap.Logger, *semver.Version) {})
	lessor := lease.NewLessor(lg, r.be, cluster, lease.LessorConfig{})
	defer lessor.Stop()
	// Do not promote this lessor: captured LeaseRevoke entries own expiry.
	var mutationObserver func(mvcc.ReplayMutation)
	if r.objects != nil {
		mutationObserver = r.observeMutation
	}
	r.kv = mvcc.NewStore(lg, r.be, lessor, mvcc.StoreConfig{
		OnReplayMutation:     mutationObserver,
		CompactionBatchLimit: cfg.CompactionBatchLimit, CompactionSleepInterval: cfg.CompactionSleepInterval,
		ReplaySynchronousCompaction: true,
		ReplayCompactionSleep: func(d time.Duration) error {
			r.scheduleErr = r.advance(r.now + d)
			return r.scheduleErr
		},
	})
	defer r.kv.Close()
	if r.scheduleErr != nil {
		return r.result, r.scheduleErr
	}
	if r.observationErr != nil {
		return r.result, r.observationErr
	}
	alarmStore, err := v3alarm.NewAlarmStore(lg, schema.NewAlarmBackend(lg, r.be))
	if err != nil {
		return res, err
	}
	tp, err := auth.NewTokenProvider(lg, "simple", func(uint64) <-chan struct{} { ch := make(chan struct{}); close(ch); return ch }, 300*time.Second)
	if err != nil {
		return res, err
	}
	as := auth.NewAuthStore(lg, schema.NewAuthBackend(lg, r.be), tp, bcrypt.DefaultCost)
	defer as.Close()
	status := &raftStatus{r: r}
	membershipApplier := apply.NewApplierMembership(lg, cluster, &snapshotServer{})
	applier := apply.NewUberApplier(lg, r.be, r.kv, alarmStore, as, lessor, cluster, status, &snapshotServer{}, ci, time.Hour, false, cfg.QuotaBackendBytes)
	// Constructor writes are a common startup boundary, separate from workload.
	backend.ReplayTick(r.be)
	r.observe = true
	r.started = time.Now()
	expected := base.Index + 1
	var lastSourceTime time.Duration
	for {
		e, sourceTime, nextErr := cfg.Source.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return r.result, fmt.Errorf("read replay input: %w", nextErr)
		}
		if e.Index != expected {
			return r.result, fmt.Errorf("entry gap: expected %d, got %d", expected, e.Index)
		}
		if sourceTime < lastSourceTime || sourceTime < 0 {
			return r.result, fmt.Errorf("nonmonotonic entry time at %d", e.Index)
		}
		expected++
		lastSourceTime = sourceTime
		if err = ctx.Err(); err != nil {
			return r.result, err
		}
		if err = r.advance(sourceTime); err != nil {
			return r.result, err
		}
		r.index = e.Index
		r.term = e.Term
		r.reason = "apply"
		ci.SetConsistentApplyingIndex(e.Index, e.Term)
		op := Operation{Index: e.Index, Term: e.Term, VirtualNS: int64(r.now), Type: "noop"}
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) > 0 {
				req, decodeErr := decodeRequest(e.Data)
				if decodeErr != nil {
					return r.result, fmt.Errorf("decode index %d: %w", e.Index, decodeErr)
				}
				op.Type = requestType(&req)
				if op.Type == "unknown" {
					return r.result, fmt.Errorf("unsupported request at index %d", e.Index)
				}
				var ar *apply.Result
				switch {
				case req.ClusterVersionSet != nil:
					membershipApplier.ClusterVersionSet(req.ClusterVersionSet, membership.ApplyBoth)
					ar = &apply.Result{}
				case req.ClusterMemberAttrSet != nil:
					membershipApplier.ClusterMemberAttrSet(req.ClusterMemberAttrSet, membership.ApplyBoth)
					ar = &apply.Result{}
				case req.DowngradeInfoSet != nil:
					membershipApplier.DowngradeInfoSet(req.DowngradeInfoSet, membership.ApplyBoth)
					ar = &apply.Result{}
				default:
					ar = applier.Apply(&req)
				}
				if r.scheduleErr != nil {
					return r.result, r.scheduleErr
				}
				if ar.Err != nil {
					op.Error = ar.Err.Error()
				}
				if req.Compaction != nil && req.Compaction.Physical && ar.Physc != nil {
					r.reason = "physical-compaction"
					r.be.ForceCommit()
				}
			}
		default:
			return r.result, fmt.Errorf("membership changes are unsupported by %s (index %d type %s)", EngineVersion, e.Index, e.Type)
		}
		if ci.ConsistentIndex() < e.Index {
			ci.SetConsistentIndex(e.Index, e.Term)
		}
		op.Revision = r.kv.Rev()
		if err = r.operations.Write(op); err != nil {
			return r.result, fmt.Errorf("write operation observation: %w", err)
		}
		r.result.OperationCount++
		if op.Error != "" {
			r.result.OperationErrorCount++
		}
		if cfg.RetainObservations {
			r.result.Operations = append(r.result.Operations, op)
		}
		r.result.AppliedEntries++
		if r.observationErr != nil {
			return r.result, r.observationErr
		}
	}
	// This single final boundary also persists a noop/read-only suffix's
	// consistent index; it is part of the declared model, not observation.
	r.reason = "finalize"
	r.be.ForceCommit()
	if r.observationErr != nil {
		return r.result, r.observationErr
	}
	digest := etcdutil.NewKVDigester()
	err = mvcc.ReplayVisitCurrent(ctx, r.kv, func(kv mvccpb.KeyValue) error {
		if cfg.RetainObservations {
			r.result.KVs = append(r.result.KVs, kv)
		}
		return digest.Add(&kv)
	})
	if err != nil {
		return r.result, err
	}
	r.result.FinalIndex = r.index
	r.result.FinalRevision = r.kv.Rev()
	r.result.KeyCount = digest.Count()
	r.result.LogicalSHA256 = digest.Sum()
	r.result.WallDurationNS = int64(time.Since(r.started))
	r.result.VirtualDurationNS = int64(r.now)
	r.observe = false
	if r.objects != nil {
		var summary analysis.Summary
		summary, err = r.objects.Finish(int64(r.now))
		if err != nil {
			return r.result, fmt.Errorf("finish object analysis: %w", err)
		}
		r.result.AnalysisDirectory = "analysis"
		r.result.Analysis = &summary
		r.analysisStarted = false
	}
	if err = errors.Join(r.operations.Close(), r.commits.Close()); err != nil {
		return r.result, fmt.Errorf("flush observations: %w", err)
	}
	r.result.CommitsSHA256 = r.commits.Hash()
	r.result.OperationsSHA256 = r.operations.Hash()
	b, err := json.MarshalIndent(r.result, "", "  ")
	if err != nil {
		return r.result, err
	}
	if err = os.WriteFile(filepath.Join(cfg.OutputDir, "report.json"), append(b, '\n'), 0600); err != nil {
		return r.result, err
	}
	return r.result, nil
}

func (r *runner) advance(target time.Duration) error {
	if target < r.now {
		target = r.now
	}
	for r.timeTick <= target {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		r.now = r.timeTick
		r.timeTick += r.cfg.BatchInterval
		previous := r.reason
		r.reason = "timer"
		backend.ReplayTick(r.be)
		r.reason = previous
		if r.observationErr != nil {
			return r.observationErr
		}
	}
	r.now = target
	if r.cfg.Speed > 0 {
		wait := time.Duration(float64(r.now)/r.cfg.Speed) - time.Since(r.started)
		if wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.ctx.Done():
				return r.ctx.Err()
			}
		}
	}
	return nil
}
func (r *runner) observeCommit(s backend.ReplayCommitStats) {
	if r.observationErr != nil {
		return
	}
	if r.objects != nil && r.analysisStarted {
		r.analysisCommits++
		if err := r.objects.Commit(analysis.Commit{Number: r.analysisCommits, Index: r.index, VirtualNS: int64(r.now), Reason: r.reason, DBSize: s.DBSize, DBSizeInUse: s.DBSizeInUse, FreePages: s.FreePages, PendingPages: s.PendingPages}); err != nil {
			r.observationErr = fmt.Errorf("analyze commit: %w", err)
			return
		}
	}
	if !r.observe {
		return
	}
	c := Commit{Number: r.result.CommitCount + 1, TxID: s.TxID, Index: r.index, VirtualNS: int64(r.now), Reason: r.reason, DBSize: s.DBSize, DBSizeInUse: s.DBSizeInUse, FreePages: s.FreePages, PendingPages: s.PendingPages}
	if r.observationErr != nil {
		return
	}
	if err := r.commits.Write(c); err != nil {
		r.observationErr = fmt.Errorf("write commit observation: %w", err)
		return
	}
	r.result.CommitCount++
	if r.cfg.RetainObservations {
		r.result.Commits = append(r.result.Commits, c)
	}
	if r.cfg.OnCommit != nil {
		r.cfg.OnCommit(c)
	}
}

func (r *runner) analysisEvent(e mvcc.ReplayMutation) analysis.Event {
	return analysis.Event{Kind: e.Kind, Key: e.Key, Value: e.Value, Revision: e.Revision, SubRevision: e.SubRevision, ValueBytes: e.ValueBytes, RecordBytes: e.RecordBytes, Tombstone: e.Tombstone, Index: r.index, VirtualNS: int64(r.now)}
}

func (r *runner) observeMutation(e mvcc.ReplayMutation) {
	if r.objects == nil || !r.analysisStarted || r.observationErr != nil {
		return
	}
	if err := r.objects.Event(r.analysisEvent(e)); err != nil {
		r.observationErr = fmt.Errorf("analyze MVCC %s at index %d: %w", e.Kind, r.index, err)
	}
}
func copyDB(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	return errors.Join(err, out.Close())
}

type raftStatus struct{ r *runner }

func (s *raftStatus) MemberID() types.ID     { return types.ID(s.r.cfg.MemberID) }
func (s *raftStatus) Leader() types.ID       { return 0 }
func (s *raftStatus) CommittedIndex() uint64 { return s.r.index }
func (s *raftStatus) AppliedIndex() uint64   { return s.r.index }
func (s *raftStatus) Term() uint64           { return s.r.term }

type snapshotServer struct{}

func (*snapshotServer) ForceSnapshot() {}
func requestType(r *pb.InternalRaftRequest) string {
	switch {
	case r.Put != nil:
		return "put"
	case r.Txn != nil:
		return "txn"
	case r.DeleteRange != nil:
		return "delete"
	case r.Range != nil:
		return "range"
	case r.Compaction != nil:
		return "compaction"
	case r.LeaseGrant != nil:
		return "lease-grant"
	case r.LeaseRevoke != nil:
		return "lease-revoke"
	case r.LeaseCheckpoint != nil:
		return "lease-checkpoint"
	case r.Alarm != nil:
		return "alarm"
	case r.ClusterVersionSet != nil:
		return "cluster-version"
	case r.ClusterMemberAttrSet != nil:
		return "member-attributes"
	case r.DowngradeInfoSet != nil:
		return "downgrade-info"
	case r.AuthEnable != nil || r.AuthDisable != nil || r.AuthStatus != nil || r.Authenticate != nil || r.AuthUserAdd != nil || r.AuthUserDelete != nil || r.AuthUserChangePassword != nil || r.AuthUserGrantRole != nil || r.AuthUserRevokeRole != nil || r.AuthUserGet != nil || r.AuthUserList != nil || r.AuthRoleAdd != nil || r.AuthRoleDelete != nil || r.AuthRoleGrantPermission != nil || r.AuthRoleRevokePermission != nil || r.AuthRoleGet != nil || r.AuthRoleList != nil:
		return "auth"
	default:
		return "unknown"
	}
}
