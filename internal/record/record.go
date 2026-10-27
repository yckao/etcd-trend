// Package record archives a member's files without joining etcd or opening its
// live backend. Snapshot acquisition is an explicit provider boundary.
package record

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"github.com/yckao/etcd-trend/internal/etcdutil"
)

type Config struct {
	WALDir          string
	OutputDir       string
	SnapshotSource  string
	SnapshotCommand []string
	PollInterval    time.Duration
	EtcdVersion     string
	Ready           func(capture.Manifest)
}

// Run starts a new recording session. Cancellation after baseline acquisition
// performs one final scan and finalizes a verified complete-prefix capture.
// Existing output directories are rejected; v1 does not resume sessions.
func Run(ctx context.Context, cfg Config) (retErr error) {
	if (cfg.SnapshotSource == "") == (len(cfg.SnapshotCommand) == 0) {
		return fmt.Errorf("choose exactly one snapshot source or snapshot command")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.EtcdVersion == "" {
		cfg.EtcdVersion = "3.6.0"
	}
	version, err := etcdutil.ParseSourceVersion(cfg.EtcdVersion)
	if err != nil {
		return err
	}
	cfg.EtcdVersion = version.Version
	walDir, err := filepath.EvalSymlinks(cfg.WALDir)
	if err != nil {
		return err
	}
	walDir, err = filepath.Abs(walDir)
	if err != nil {
		return err
	}
	out, err := filepath.Abs(cfg.OutputDir)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(out))
	if err != nil {
		return fmt.Errorf("resolve capture parent: %w", err)
	}
	out = filepath.Join(parent, filepath.Base(out))
	if inside(walDir, out) {
		return fmt.Errorf("capture output must be outside the source WAL directory")
	}
	if cfg.SnapshotSource != "" {
		source, err := filepath.EvalSymlinks(cfg.SnapshotSource)
		if err != nil {
			return err
		}
		source, err = filepath.Abs(source)
		if err != nil {
			return err
		}
		live := filepath.Join(filepath.Dir(walDir), "snap", "db")
		if source == live || inside(walDir, source) {
			return fmt.Errorf("snapshot source is in the live member data directory; provide an immutable snapshot copy")
		}
		cfg.SnapshotSource = source
	}
	if err = os.Mkdir(out, 0700); err != nil {
		return fmt.Errorf("create new capture directory: %w", err)
	}
	if err = os.Mkdir(filepath.Join(out, "wal"), 0700); err != nil {
		return err
	}
	m := capture.Manifest{Version: capture.FormatVersion, EtcdVersion: cfg.EtcdVersion, State: "recording", StartedAt: time.Now().UTC()}
	defer func() {
		if retErr != nil {
			m.State, m.Error, m.FinishedAt = "failed", retErr.Error(), time.Now().UTC()
			_ = capture.WriteManifest(out, m)
		}
	}()
	if err = capture.WriteManifest(out, m); err != nil {
		return err
	}
	a, err := newArchiver(walDir, out)
	if err != nil {
		return err
	}
	defer a.close()
	if err = a.scan(); err != nil {
		return err
	}
	m.ClusterID, m.MemberID = a.clusterID, a.memberID
	_ = a.journal("started", "WAL acquisition started before snapshot provider")
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- a.scan()
				return
			case <-ticker.C:
				if err := a.scan(); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	workerStopped := false
	defer func() {
		if !workerStopped {
			close(stop)
			<-done
		}
	}()
	snapshotPath := filepath.Join(out, ".baseline.tmp")
	if cfg.SnapshotSource != "" {
		err = copyImmutable(cfg.SnapshotSource, snapshotPath)
	} else {
		cmd := exec.CommandContext(ctx, cfg.SnapshotCommand[0], cfg.SnapshotCommand[1:]...)
		cmd.Env = append(os.Environ(), "CAPTURE_SNAPSHOT_PATH="+snapshotPath)
		// Snapshot providers are trusted, explicitly configured executables. Their
		// output stays local; do not include potentially sensitive stdout in errors.
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		err = cmd.Run()
		if err != nil {
			err = fmt.Errorf("snapshot provider command failed: %w", err)
		}
	}
	if err != nil {
		return err
	}
	outputInfo, err := os.Lstat(snapshotPath)
	if err != nil {
		return err
	}
	if !outputInfo.Mode().IsRegular() {
		return fmt.Errorf("snapshot provider must create a regular file, not a link")
	}
	if liveInfo, statErr := os.Stat(filepath.Join(filepath.Dir(walDir), "snap", "db")); statErr == nil && os.SameFile(outputInfo, liveInfo) {
		return fmt.Errorf("snapshot provider returned the live backend inode")
	}
	snapshot, err := capture.InspectSnapshot(snapshotPath)
	if err != nil {
		return err
	}
	if err = os.Chmod(snapshotPath, 0600); err != nil {
		return err
	}
	// An external provider may have closed its output without syncing it.
	snapshotFile, err := os.OpenFile(snapshotPath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = snapshotFile.Sync()
	if closeErr := snapshotFile.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(snapshotPath, filepath.Join(out, "baseline.db")); err != nil {
		return err
	}
	snapshot.File, snapshot.CapturedAt = "baseline.db", time.Now().UTC()
	snapshot.Provider = "immutable-file"
	if len(cfg.SnapshotCommand) != 0 {
		snapshot.Provider = "command"
	}
	m.Snapshot = snapshot
	if err = capture.WriteManifest(out, m); err != nil {
		return err
	}
	if cfg.Ready != nil {
		cfg.Ready(m)
	}
	select {
	case err = <-done:
		workerStopped = true
		if err == nil {
			err = fmt.Errorf("WAL archiver stopped unexpectedly")
		}
		return err
	case <-ctx.Done():
		close(stop)
		err = <-done
		workerStopped = true
		if err != nil {
			return err
		}
	}
	m.ClusterID, m.MemberID = a.clusterID, a.memberID
	m.WALFiles, err = a.finalize()
	if err != nil {
		return err
	}
	m.LastCommittedIndex = a.lastCommit
	if a.lastCommit < m.Snapshot.Index {
		return fmt.Errorf("captured WAL commit %d predates baseline index %d", a.lastCommit, m.Snapshot.Index)
	}
	if a.lastCommit > m.Snapshot.Index && a.lastIndex < a.lastCommit {
		return fmt.Errorf("captured WAL ends at index %d before committed index %d", a.lastIndex, a.lastCommit)
	}
	m.Warnings = a.warnings
	m.State, m.FinishedAt = "complete", time.Now().UTC()
	if err = a.journal("stopped", "Captured prefix finalized; later source writes are outside this session"); err != nil {
		return err
	}
	if err = a.sync(); err != nil {
		return err
	}
	if err = capture.WriteManifest(out, m); err != nil {
		return err
	}
	return nil
}

func inside(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func copyImmutable(source, dest string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	before, err := in.Stat()
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() {
		return fmt.Errorf("snapshot source must be a regular immutable file")
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	if err = out.Close(); copyErr == nil {
		copyErr = err
	}
	if copyErr != nil {
		return copyErr
	}
	after, err := in.Stat()
	if err != nil {
		return err
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return fmt.Errorf("snapshot source changed while being copied")
	}
	return nil
}

func appendJSON(file *os.File, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = file.Write(append(b, '\n'))
	return err
}
