package record

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/yckao/etcd-trend/internal/capture"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

const maxFrameBytes = 256 << 20

type segment struct {
	name     string
	seq      uint64
	source   *os.File
	identity os.FileInfo
	output   *os.File
	offset   int64
	crc      uint32
	sealed   bool
	pending  string
	digest   hash.Hash
}

type archiver struct {
	sourceDir     string
	dest          string
	segments      []*segment
	bySequence    map[uint64]*segment
	firstUnsealed int
	anchors       *os.File
	journalFile   *os.File
	metadata      []byte
	memberID      uint64
	clusterID     uint64
	warnings      []string
	lastCommit    uint64
	lastIndex     uint64
}

func newArchiver(sourceDir, dest string) (*archiver, error) {
	a := &archiver{sourceDir: sourceDir, dest: dest, bySequence: map[uint64]*segment{}}
	var err error
	a.anchors, err = os.OpenFile(filepath.Join(dest, "anchors.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	a.journalFile, err = os.OpenFile(filepath.Join(dest, "journal.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		a.anchors.Close()
		return nil, err
	}
	return a, nil
}

func (a *archiver) scan() error {
	listing, err := os.ReadDir(a.sourceDir)
	if err != nil {
		return err
	}
	var names []string
	for _, entry := range listing {
		if entry.IsDir() {
			continue
		}
		if _, _, err := capture.ParseWALName(entry.Name()); err == nil {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 && len(a.segments) == 0 {
		return fmt.Errorf("no WAL segments in source directory")
	}
	for _, name := range names {
		seq, _, _ := capture.ParseWALName(name)
		known := a.bySequence[seq]
		if known != nil {
			info, err := os.Stat(filepath.Join(a.sourceDir, name))
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			if name != known.name || !os.SameFile(known.identity, info) {
				return fmt.Errorf("source WAL identity changed at sequence %d", seq)
			}
			continue
		}
		if len(a.segments) > 0 && seq != a.segments[len(a.segments)-1].seq+1 {
			return fmt.Errorf("source WAL sequence gap: expected %d, found %d", a.segments[len(a.segments)-1].seq+1, seq)
		}
		f, err := os.Open(filepath.Join(a.sourceDir, name))
		if os.IsNotExist(err) {
			return fmt.Errorf("source WAL was purged before acquisition: %s", name)
		}
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if !info.Mode().IsRegular() {
			f.Close()
			return fmt.Errorf("source WAL is not a regular file")
		}
		out, err := os.OpenFile(filepath.Join(a.dest, "wal", name+".part"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			f.Close()
			return err
		}
		s := &segment{name: name, seq: seq, source: f, identity: info, output: out, digest: sha256.New()}
		a.segments = append(a.segments, s)
		a.bySequence[seq] = s
	}
	for i := a.firstUnsealed; i < len(a.segments); i++ {
		s := a.segments[i]
		closed := i < len(a.segments)-1
		before := s.offset
		if err = a.readFrames(s, closed); err != nil {
			return err
		}
		if s.offset > before {
			if err = s.output.Sync(); err != nil {
				return err
			}
			if err = appendJSON(a.anchors, capture.Anchor{ObservedAt: time.Now().UTC(), Segment: s.name, Offset: s.offset}); err != nil {
				return err
			}
		}
		if closed {
			if err = a.seal(s); err != nil {
				return err
			}
			a.firstUnsealed = i + 1
		}
	}
	return a.anchors.Sync()
}

// readFrames copies complete CRC-validated records only. A reader racing the
// active writer retries an incomplete frame on its next poll. Closed segments
// must validate completely; they are never repaired or truncated at the source.
func (a *archiver) readFrames(s *segment, closed bool) error {
	info, err := s.source.Stat()
	if err != nil {
		return err
	}
	if info.Size() < s.offset {
		return fmt.Errorf("source WAL shrank behind captured offset: %s", s.name)
	}
	s.pending = ""
	for {
		var header [8]byte
		n, err := s.source.ReadAt(header[:], s.offset)
		if err == io.EOF && n == 0 {
			return nil
		}
		if err != nil {
			if closed {
				return fmt.Errorf("truncated closed WAL header %s: %w", s.name, err)
			}
			s.pending = "partial frame header"
			return nil
		}
		encoded := binary.LittleEndian.Uint64(header[:])
		if encoded == 0 {
			if closed {
				return fmt.Errorf("unexpected zero frame in rotated WAL %s at %d", s.name, s.offset)
			}
			return nil
		}
		size := encoded & ((uint64(1) << 56) - 1)
		padding := uint64(0)
		if encoded>>63 != 0 {
			padding = (encoded >> 56) & 7
		}
		if size > maxFrameBytes || size == 0 {
			return fmt.Errorf("invalid or oversized WAL frame in %s at %d: %d bytes", s.name, s.offset, size)
		}
		frame := make([]byte, 8+size+padding)
		copy(frame, header[:])
		if _, err = s.source.ReadAt(frame[8:], s.offset+8); err != nil {
			if closed {
				return fmt.Errorf("truncated closed WAL frame %s: %w", s.name, err)
			}
			s.pending = "partial frame body"
			return nil
		}
		var rec walpb.Record
		if err = rec.Unmarshal(frame[8 : 8+size]); err != nil {
			if closed {
				return fmt.Errorf("invalid closed WAL record %s: %w", s.name, err)
			}
			s.pending = "incomplete or invalid frame"
			return nil
		}
		nextCRC := s.crc
		if rec.Type == wal.CrcType {
			if s.offset != 0 && rec.Crc != s.crc {
				return fmt.Errorf("CRC chain mismatch in %s", s.name)
			}
			if s.offset == 0 {
				if previous, ok := a.bySequence[s.seq-1]; s.seq > 0 && ok && rec.Crc != previous.crc {
					return fmt.Errorf("CRC chain mismatch between WAL segments at %s", s.name)
				}
			}
			nextCRC = rec.Crc
		} else {
			if s.offset == 0 {
				return fmt.Errorf("WAL segment has no initial CRC: %s", s.name)
			}
			nextCRC = crc32.Update(s.crc, crc32.MakeTable(crc32.Castagnoli), rec.Data)
			if err = rec.Validate(nextCRC); err != nil {
				if closed {
					return fmt.Errorf("CRC mismatch in closed WAL %s: %w", s.name, err)
				}
				s.pending = "incomplete or CRC-invalid frame"
				return nil
			}
		}
		if rec.Type == wal.MetadataType {
			if a.metadata != nil && !bytes.Equal(a.metadata, rec.Data) {
				return fmt.Errorf("source WAL member or cluster identity changed")
			}
			var md etcdserverpb.Metadata
			if err = md.Unmarshal(rec.Data); err != nil {
				return err
			}
			a.metadata = append(a.metadata[:0], rec.Data...)
			a.clusterID, a.memberID = md.ClusterID, md.NodeID
		}
		if rec.Type == wal.EntryType {
			var entry raftpb.Entry
			if err = entry.Unmarshal(rec.Data); err != nil {
				return err
			}
			a.lastIndex = entry.Index
		}
		if rec.Type == wal.StateType {
			var state raftpb.HardState
			if err = state.Unmarshal(rec.Data); err != nil {
				return err
			}
			if state.Commit < a.lastCommit {
				return fmt.Errorf("source WAL commit index regressed")
			}
			a.lastCommit = state.Commit
		}
		if _, err = s.output.Write(frame); err != nil {
			return err
		}
		_, _ = s.digest.Write(frame)
		s.offset += int64(len(frame))
		s.crc = nextCRC
	}
}

func (a *archiver) seal(s *segment) error {
	if err := s.output.Sync(); err != nil {
		return err
	}
	if err := s.output.Close(); err != nil {
		return err
	}
	s.output = nil
	if err := s.source.Close(); err != nil {
		return err
	}
	s.source = nil
	if err := os.Rename(filepath.Join(a.dest, "wal", s.name+".part"), filepath.Join(a.dest, "wal", s.name)); err != nil {
		return err
	}
	s.sealed = true
	return a.journal("segment_sealed", s.name)
}

func (a *archiver) finalize() ([]capture.WALFile, error) {
	var files []capture.WALFile
	for _, s := range a.segments {
		sealed := s.sealed
		if !sealed {
			if s.pending != "" {
				a.warnings = append(a.warnings, fmt.Sprintf("%s: excluded trailing %s", s.name, s.pending))
			}
			if err := a.seal(s); err != nil {
				return nil, err
			}
		}
		files = append(files, capture.WALFile{Name: s.name, Sequence: s.seq, Size: s.offset, SHA256: hex.EncodeToString(s.digest.Sum(nil)), Sealed: sealed})
	}
	dir, err := os.Open(filepath.Join(a.dest, "wal"))
	if err != nil {
		return nil, err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (a *archiver) journal(kind, detail string) error {
	return appendJSON(a.journalFile, capture.JournalEvent{At: time.Now().UTC(), Type: kind, Detail: detail})
}

func (a *archiver) sync() error {
	if err := a.anchors.Sync(); err != nil {
		return err
	}
	return a.journalFile.Sync()
}

func (a *archiver) close() {
	for _, s := range a.segments {
		if s.source != nil {
			_ = s.source.Close()
		}
		if s.output != nil {
			_ = s.output.Close()
		}
	}
	if a.anchors != nil {
		_ = a.anchors.Close()
	}
	if a.journalFile != nil {
		_ = a.journalFile.Close()
	}
}
