// Package compatfixture qualifies a capture against an actual external etcd
// release. It owns only a new directory and the loopback process it starts.
// Production recorder and analyzer commands do not import this package.
package compatfixture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/internal/fixture"
	"github.com/yckao/etcd-trend/internal/record"
	"github.com/yckao/etcd-trend/internal/replay"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Config struct {
	Binary      string
	Dir         string
	Workload    fixture.Workload
	Speeds      []float64
	CaptureOnly bool
}

type BinaryInfo struct {
	Path          string `json:"path"`
	Version       string `json:"version"`
	SHA256        string `json:"sha256"`
	VersionOutput string `json:"version_output"`
}

type ReplaySummary struct {
	Speed     float64 `json:"speed"`
	Revision  int64   `json:"revision"`
	Digest    string  `json:"logical_sha256"`
	Entries   int     `json:"applied_entries"`
	Commits   int     `json:"commits"`
	WallNS    int64   `json:"wall_duration_ns"`
	VirtualNS int64   `json:"virtual_duration_ns"`
	Report    string  `json:"report"`
}

type Summary struct {
	Binary                BinaryInfo      `json:"binary"`
	Status                string          `json:"status"`
	Capture               string          `json:"capture"`
	SourceRevision        int64           `json:"source_revision"`
	SourceKeyCount        int             `json:"source_key_count"`
	SourceLogicalSHA256   string          `json:"source_logical_sha256"`
	CompactedRevision     int64           `json:"compacted_revision"`
	WALSegments           int             `json:"wal_segments"`
	CommittedEntries      int             `json:"committed_entries"`
	OperationCounts       map[string]int  `json:"operation_counts,omitempty"`
	Replays               []ReplaySummary `json:"replays,omitempty"`
	CommitBoundariesEqual bool            `json:"commit_boundaries_equal"`
	SpaceCurvesEqual      bool            `json:"space_curves_equal"`
	CaptureUnchanged      bool            `json:"capture_unchanged"`
	Error                 string          `json:"error,omitempty"`
}

var versionPattern = regexp.MustCompile(`(?m)^etcd Version: (3\.(?:4|5|6)\.\d+)(?:\s|$)`)

func InspectBinary(ctx context.Context, binary string) (BinaryInfo, error) {
	var info BinaryInfo
	path, err := filepath.Abs(binary)
	if err != nil {
		return info, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return info, err
	}
	if !stat.Mode().IsRegular() {
		return info, fmt.Errorf("etcd binary is not a regular file")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(checkCtx, path, "--version")
	command.Env = append(os.Environ(), "ETCD_UNSUPPORTED_ARCH="+runtime.GOARCH)
	out, err := command.CombinedOutput()
	if err != nil {
		return info, fmt.Errorf("inspect etcd binary: %w: %s", err, strings.TrimSpace(string(out)))
	}
	m := versionPattern.FindStringSubmatch(string(out))
	if len(m) != 2 {
		return info, fmt.Errorf("expected an etcd 3.4, 3.5, or 3.6 release, got %q", strings.TrimSpace(string(out)))
	}
	sha, err := capture.FileSHA256(path)
	if err != nil {
		return info, err
	}
	return BinaryInfo{Path: path, Version: m[1], SHA256: sha, VersionOutput: strings.TrimSpace(string(out))}, nil
}

// Run stops and restarts only its own synthetic member to acquire the baseline.
// It removes the source's original path before replay to prove capture portability.
func Run(ctx context.Context, cfg Config) (summary Summary, err error) {
	if cfg.Binary == "" || cfg.Dir == "" {
		return summary, fmt.Errorf("binary and new output directory are required")
	}
	summary.Binary, err = InspectBinary(ctx, cfg.Binary)
	if err != nil {
		return summary, err
	}
	if err = os.Mkdir(cfg.Dir, 0700); err != nil {
		return summary, err
	}
	defer func() {
		if err != nil {
			summary.Status, summary.Error = "failed", err.Error()
		}
		b, marshalErr := json.MarshalIndent(summary, "", "  ")
		if marshalErr == nil {
			writeErr := os.WriteFile(filepath.Join(cfg.Dir, "summary.json"), append(b, '\n'), 0600)
			if err == nil {
				err = writeErr
			}
		}
	}()
	if len(cfg.Speeds) == 0 {
		cfg.Speeds = []float64{1, 100, 0}
	}
	sourceDir := filepath.Join(cfg.Dir, "source")
	if err = os.Mkdir(sourceDir, 0700); err != nil {
		return summary, err
	}
	clientURL, err := freeURL()
	if err != nil {
		return summary, err
	}
	peerURL, err := freeURL()
	if err != nil {
		return summary, err
	}
	args := []string{
		"--name", "etcd-trend-compat", "--data-dir", filepath.Join(sourceDir, "data"),
		"--listen-client-urls", clientURL, "--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "etcd-trend-compat=" + peerURL,
		"--initial-cluster-token", "etcd-trend-compat-" + filepath.Base(cfg.Dir),
		"--heartbeat-interval", "25", "--election-timeout", "250",
		"--snapshot-count", "50", "--max-wals", "0", "--max-snapshots", "0",
		"--logger", "zap", "--log-level", "error",
	}
	proc, client, err := launch(ctx, summary.Binary, args, clientURL, sourceDir)
	if err != nil {
		return summary, err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
		if proc != nil {
			_ = proc.stop()
		}
	}()
	_, err = client.Txn(ctx).Then(
		clientv3.OpPut("/registry/configmaps/default/stable", "baseline"),
		clientv3.OpPut("/registry/widgets/default/hot", "initial"),
		clientv3.OpPut("/registry/pods/default/delete-me", "old-pod"),
	).Commit()
	if err != nil {
		return summary, fmt.Errorf("seed source: %w", err)
	}
	client.Close()
	client = nil
	if err = proc.stop(); err != nil {
		return summary, err
	}
	proc = nil
	baseline := filepath.Join(sourceDir, "baseline.db")
	if err = copyFile(filepath.Join(sourceDir, "data", "member", "snap", "db"), baseline); err != nil {
		return summary, err
	}
	proc, client, err = launch(ctx, summary.Binary, args, clientURL, sourceDir)
	if err != nil {
		return summary, err
	}
	summary.Capture = filepath.Join(cfg.Dir, "capture")
	recCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make(chan struct{})
	recDone := make(chan error, 1)
	go func() {
		recDone <- record.Run(recCtx, record.Config{WALDir: filepath.Join(sourceDir, "data", "member", "wal"), OutputDir: summary.Capture,
			SnapshotSource: baseline, PollInterval: 5 * time.Millisecond, EtcdVersion: summary.Binary.Version,
			Ready: func(capture.Manifest) { close(ready) }})
	}()
	select {
	case <-ready:
	case err = <-recDone:
		return summary, fmt.Errorf("recorder startup: %w", err)
	case <-ctx.Done():
		cancel()
		<-recDone
		return summary, ctx.Err()
	}
	expected, workloadErr := (&fixture.Instance{Client: client}).Exercise(ctx, cfg.Workload)
	client.Close()
	client = nil
	stopErr := proc.stop()
	proc = nil
	cancel()
	recordErr := <-recDone
	if err = errors.Join(workloadErr, stopErr, recordErr); err != nil {
		return summary, err
	}
	if err = os.Rename(sourceDir, filepath.Join(cfg.Dir, "source-stopped")); err != nil {
		return summary, err
	}
	d, err := capture.Load(summary.Capture)
	if err != nil {
		return summary, err
	}
	kvs := make([]mvccpb.KeyValue, len(expected.KVs))
	for i, kv := range expected.KVs {
		kvs[i] = *kv
	}
	summary.SourceRevision, summary.SourceKeyCount = expected.Revision, len(kvs)
	summary.SourceLogicalSHA256 = etcdutil.HashKVs(kvs)
	summary.CompactedRevision = expected.CompactedRevision
	summary.WALSegments, summary.CommittedEntries = len(d.Manifest.WALFiles), len(d.Entries)
	summary.OperationCounts = map[string]int{}
	summary.Status = "captured"
	if cfg.CaptureOnly {
		return summary, nil
	}
	before, err := hashes(summary.Capture, d.Manifest)
	if err != nil {
		return summary, err
	}
	summary.CommitBoundariesEqual, summary.SpaceCurvesEqual = true, true
	var reference replay.Result
	for n, speed := range cfg.Speeds {
		out := filepath.Join(cfg.Dir, fmt.Sprintf("replay-%d", n))
		result, runErr := replay.Run(ctx, replay.Config{BaselinePath: d.BaselinePath, Entries: d.Entries, EntryTimes: d.EntryTimes,
			OutputDir: out, Speed: speed, SourceVersion: summary.Binary.Version, RetainObservations: true,
			MemberID: d.Manifest.MemberID, ClusterID: d.Manifest.ClusterID})
		if runErr != nil {
			return summary, fmt.Errorf("replay source %s speed %g: %w", summary.Binary.Version, speed, runErr)
		}
		if result.FinalRevision != expected.Revision || result.LogicalSHA256 != summary.SourceLogicalSHA256 {
			return summary, fmt.Errorf("source %s replay diverged: revision %d/%d, digest %s/%s", summary.Binary.Version,
				result.FinalRevision, expected.Revision, result.LogicalSHA256, summary.SourceLogicalSHA256)
		}
		if n == 0 {
			reference = result
			for _, op := range result.Operations {
				summary.OperationCounts[op.Type]++
			}
		} else {
			if !sameBoundaries(reference.Commits, result.Commits) {
				summary.CommitBoundariesEqual = false
				return summary, fmt.Errorf("playback speed changed modeled commit boundaries")
			}
			if !reflect.DeepEqual(reference.Commits, result.Commits) {
				summary.SpaceCurvesEqual = false
			}
		}
		summary.Replays = append(summary.Replays, ReplaySummary{Speed: speed, Revision: result.FinalRevision, Digest: result.LogicalSHA256,
			Entries: result.AppliedEntries, Commits: len(result.Commits), WallNS: result.WallDurationNS,
			VirtualNS: result.VirtualDurationNS, Report: filepath.Join(out, "report.json")})
	}
	after, err := hashes(summary.Capture, d.Manifest)
	if err != nil {
		return summary, err
	}
	summary.CaptureUnchanged = reflect.DeepEqual(before, after)
	if !summary.CaptureUnchanged {
		return summary, fmt.Errorf("analyzer changed capture artifacts")
	}
	summary.Status = "passed"
	return summary, nil
}

type process struct {
	cmd     *exec.Cmd
	done    <-chan error
	log     *os.File
	stopped bool
	stopErr error
}

func launch(ctx context.Context, binary BinaryInfo, args []string, endpoint, dir string) (*process, *clientv3.Client, error) {
	log, err := os.OpenFile(filepath.Join(dir, "etcd.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(binary.Path, args...)
	cmd.Stdout, cmd.Stderr = log, log
	// Older branches restrict architectures even when Go can build them. This
	// setting belongs solely to this isolated compatibility fixture.
	cmd.Env = append(os.Environ(), "ETCD_UNSUPPORTED_ARCH="+runtime.GOARCH)
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	p := &process{cmd: cmd, done: done, log: log}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second, Logger: zap.NewNop()})
	if err != nil {
		_ = p.stop()
		return nil, nil, err
	}
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		status, statusErr := client.Status(requestCtx, endpoint)
		cancel()
		if statusErr == nil && status.Leader != 0 {
			if status.Version != binary.Version {
				client.Close()
				_ = p.stop()
				return nil, nil, fmt.Errorf("server version %s differs from binary %s", status.Version, binary.Version)
			}
			return p, client, nil
		}
		select {
		case exitErr := <-done:
			client.Close()
			log.Close()
			return nil, nil, fmt.Errorf("fixture etcd exited: %v; see %s", exitErr, filepath.Join(dir, "etcd.log"))
		case <-ctx.Done():
			client.Close()
			_ = p.stop()
			return nil, nil, ctx.Err()
		case <-deadline.C:
			client.Close()
			_ = p.stop()
			return nil, nil, fmt.Errorf("fixture readiness timeout; see %s", filepath.Join(dir, "etcd.log"))
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (p *process) stop() error {
	if p == nil {
		return nil
	}
	if p.stopped {
		return p.stopErr
	}
	p.stopped = true
	err := p.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		p.stopErr = err
		return p.stopErr
	}
	select {
	case err = <-p.done:
		// etcd's interrupt handler closes the member, then re-raises the signal
		// on itself. That expected exit is the successful graceful-stop path.
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGTERM {
				err = nil
			}
		}
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		err = fmt.Errorf("fixture did not shut down gracefully")
	}
	p.stopErr = errors.Join(err, p.log.Close())
	return p.stopErr
}

func freeURL() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	endpoint := "http://" + l.Addr().String()
	return endpoint, l.Close()
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	return errors.Join(err, out.Close())
}

func sameBoundaries(a, b []replay.Commit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].VirtualNS != b[i].VirtualNS || a[i].Reason != b[i].Reason {
			return false
		}
	}
	return true
}

func hashes(dir string, m capture.Manifest) (map[string]string, error) {
	names := []string{"manifest.json", "baseline.db", "anchors.jsonl", "journal.jsonl"}
	for _, w := range m.WALFiles {
		names = append(names, filepath.Join("wal", w.Name))
	}
	r := map[string]string{}
	for _, name := range names {
		hash, err := capture.FileSHA256(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		r[name] = hash
	}
	return r, nil
}
