// Package capture defines the portable, immutable input to an offline replay.
package capture

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

const FormatVersion = 1

type Snapshot struct {
	File       string    `json:"file"`
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	Index      uint64    `json:"consistent_index"`
	Term       uint64    `json:"term"`
	Revision   int64     `json:"revision"`
	CapturedAt time.Time `json:"captured_at"`
	Provider   string    `json:"provider"`
}

type WALFile struct {
	Name     string `json:"name"`
	Sequence uint64 `json:"sequence"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	Sealed   bool   `json:"sealed"`
}

type Manifest struct {
	Version            int       `json:"version"`
	EtcdVersion        string    `json:"etcd_version"`
	State              string    `json:"state"`
	StartedAt          time.Time `json:"started_at"`
	FinishedAt         time.Time `json:"finished_at,omitempty"`
	ClusterID          uint64    `json:"cluster_id"`
	MemberID           uint64    `json:"member_id"`
	LastCommittedIndex uint64    `json:"last_committed_index"`
	Snapshot           Snapshot  `json:"snapshot"`
	WALFiles           []WALFile `json:"wal_files"`
	Error              string    `json:"error,omitempty"`
	Warnings           []string  `json:"warnings,omitempty"`
}

// Anchor timestamps an observed complete WAL prefix. It is not a server commit
// timestamp. Entries first observed in the same poll share a time anchor.
type Anchor struct {
	ObservedAt time.Time `json:"observed_at"`
	Segment    string    `json:"segment"`
	Offset     int64     `json:"offset"`
}

type JournalEvent struct {
	At     time.Time `json:"at"`
	Type   string    `json:"type"`
	Detail string    `json:"detail,omitempty"`
}

type Dataset struct {
	Manifest     Manifest
	BaselinePath string
	Entries      []raftpb.Entry
	HardState    raftpb.HardState
	Anchors      []Anchor
	EntryTimes   map[uint64]time.Duration
}

// WriteManifest replaces the manifest only after its new contents reach disk.
func WriteManifest(dir string, m Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := filepath.Join(dir, "manifest.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(dir, "manifest.json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func ReadManifest(dir string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	if m.Version != FormatVersion {
		return m, fmt.Errorf("unsupported capture format %d", m.Version)
	}
	return m, nil
}

func FileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
