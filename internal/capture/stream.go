package capture

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yckao/etcd-trend/internal/etcdutil"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal/walpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

// The disk index contains locations, not payloads. Its size is linear in entry
// count; resident memory is bounded by the largest WAL record and segment metadata.
const EntryIndexBytes = 48
const AnchorIndexBytes = 16
const MaxRecordBytes = 256 << 20

type Prepared struct {
	Manifest     Manifest
	BaselinePath string
	HardState    raftpb.HardState
	Count        uint64
	IndexBytes   int64
	AnchorBytes  int64
	IndexDir     string
	captureDir   string
	files        []WALFile
}

type indexRecord struct {
	segment     uint32
	previousCRC uint32
	offset      uint64
	length      uint64
	timeNS      int64
	term        uint64
	typeID      uint32
}

func (r indexRecord) encode() [EntryIndexBytes]byte {
	var b [EntryIndexBytes]byte
	binary.LittleEndian.PutUint32(b[0:4], r.segment)
	binary.LittleEndian.PutUint32(b[4:8], r.previousCRC)
	binary.LittleEndian.PutUint64(b[8:16], r.offset)
	binary.LittleEndian.PutUint64(b[16:24], r.length)
	binary.LittleEndian.PutUint64(b[24:32], uint64(r.timeNS))
	binary.LittleEndian.PutUint64(b[32:40], r.term)
	binary.LittleEndian.PutUint32(b[40:44], r.typeID)
	return b
}

func decodeIndex(b []byte) indexRecord {
	return indexRecord{segment: binary.LittleEndian.Uint32(b[:4]), previousCRC: binary.LittleEndian.Uint32(b[4:8]), offset: binary.LittleEndian.Uint64(b[8:16]), length: binary.LittleEndian.Uint64(b[16:24]), timeNS: int64(binary.LittleEndian.Uint64(b[24:32])), term: binary.LittleEndian.Uint64(b[32:40]), typeID: binary.LittleEndian.Uint32(b[40:44])}
}

// Prepare validates all source hashes and WAL CRCs before exposing a replay
// stream. Superseded suffixes are truncated in a fixed-width on-disk index.
// indexDir must be a new directory outside the capture. Callers own its cleanup.
func Prepare(ctx context.Context, dir, indexDir string) (_ *Prepared, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	indexDir, err = filepath.Abs(indexDir)
	if err != nil {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(indexDir))
	if err != nil {
		return nil, err
	}
	indexDir = filepath.Join(parent, filepath.Base(indexDir))
	if rel, err := filepath.Rel(dir, indexDir); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("index directory must be outside immutable capture")
	}
	m, err := ReadManifest(dir)
	if err != nil {
		return nil, err
	}
	if m.State != "complete" {
		return nil, fmt.Errorf("capture state is %q: %s", m.State, m.Error)
	}
	version, err := etcdutil.ParseSourceVersion(m.EtcdVersion)
	if err != nil {
		return nil, err
	}
	m.EtcdVersion = version.Version
	if m.Snapshot.File != "baseline.db" {
		return nil, fmt.Errorf("unsupported snapshot path %q", m.Snapshot.File)
	}
	p := &Prepared{Manifest: m, BaselinePath: filepath.Join(dir, m.Snapshot.File), IndexDir: indexDir, captureDir: dir, files: append([]WALFile(nil), m.WALFiles...)}
	snapshot, err := InspectSnapshot(p.BaselinePath)
	if err != nil {
		return nil, err
	}
	if snapshot.SHA256 != m.Snapshot.SHA256 || snapshot.Size != m.Snapshot.Size || snapshot.Index != m.Snapshot.Index || snapshot.Term != m.Snapshot.Term || snapshot.Revision != m.Snapshot.Revision {
		return nil, fmt.Errorf("baseline differs from capture manifest")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	sort.Slice(p.files, func(i, j int) bool { return p.files[i].Sequence < p.files[j].Sequence })
	if len(p.files) == 0 {
		return nil, fmt.Errorf("capture contains no WAL segments")
	}
	if uint64(len(p.files)) > math.MaxUint32 {
		return nil, fmt.Errorf("too many WAL segments")
	}
	for i, f := range p.files {
		seq, _, err := ParseWALName(f.Name)
		if err != nil {
			return nil, err
		}
		if seq != f.Sequence || (i > 0 && seq != p.files[i-1].Sequence+1) {
			return nil, fmt.Errorf("WAL sequence gap or invalid manifest at %s", f.Name)
		}
	}
	if err = os.Mkdir(indexDir, 0700); err != nil {
		return nil, fmt.Errorf("create new index directory: %w", err)
	}
	ranges, anchorBytes, err := prepareAnchors(ctx, dir, indexDir, m.StartedAt, p.files, &p.Manifest)
	if err != nil {
		return nil, err
	}
	p.AnchorBytes = anchorBytes
	anchorFile, err := os.Open(filepath.Join(indexDir, "anchors.idx"))
	if err != nil {
		return nil, err
	}
	defer anchorFile.Close()
	idx, err := os.OpenFile(filepath.Join(indexDir, "entries.idx"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer idx.Close()
	writer := bufio.NewWriterSize(idx, 64<<10)
	var count uint64
	var previousCRC uint32
	var metadata []byte
	var state raftpb.HardState
	for fileNumber, f := range p.files {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		input, err := os.Open(filepath.Join(dir, "wal", f.Name))
		if err != nil {
			return nil, err
		}
		info, err := input.Stat()
		if err != nil {
			input.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() != f.Size {
			input.Close()
			return nil, fmt.Errorf("WAL size mismatch: %s", f.Name)
		}
		hr := &hashingReader{File: input, hash: sha256.New(), ctx: ctx}
		decoder := wal.NewDecoder(hr)
		decoder.UpdateCRC(previousCRC)
		anchors := newAnchorCursor(anchorFile, ranges[fileNumber])
		first := true
		for {
			if err = ctx.Err(); err != nil {
				input.Close()
				return nil, err
			}
			offset, seed := decoder.LastOffset(), decoder.LastCRC()
			if err = checkFrameBound(input, offset); err != nil {
				input.Close()
				return nil, fmt.Errorf("%s: %w", f.Name, err)
			}
			var rec walpb.Record
			err = decoder.Decode(&rec)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				input.Close()
				return nil, fmt.Errorf("decode %s at %d: %w", f.Name, offset, err)
			}
			if first && rec.Type != wal.CrcType {
				input.Close()
				return nil, fmt.Errorf("missing initial CRC record: %s", f.Name)
			}
			first = false
			switch rec.Type {
			case wal.CrcType:
				if (fileNumber > 0 || offset > 0) && rec.Crc != decoder.LastCRC() {
					input.Close()
					return nil, fmt.Errorf("WAL CRC chain mismatch: %s", f.Name)
				}
				decoder.UpdateCRC(rec.Crc)
			case wal.MetadataType:
				if metadata != nil && !bytes.Equal(metadata, rec.Data) {
					input.Close()
					return nil, fmt.Errorf("WAL member or cluster changed: %s", f.Name)
				}
				metadata = append(metadata[:0], rec.Data...)
			case wal.StateType:
				var next raftpb.HardState
				if err = next.Unmarshal(rec.Data); err != nil {
					input.Close()
					return nil, err
				}
				if next.Commit < state.Commit || next.Term < state.Term {
					input.Close()
					return nil, fmt.Errorf("WAL HardState regressed")
				}
				state = next
			case wal.EntryType:
				var e raftpb.Entry
				if err = e.Unmarshal(rec.Data); err != nil {
					input.Close()
					return nil, err
				}
				if e.Index <= m.Snapshot.Index {
					continue
				}
				position := e.Index - m.Snapshot.Index - 1
				if position > count {
					input.Close()
					return nil, fmt.Errorf("WAL entry gap: expected index %d, found %d", m.Snapshot.Index+count+1, e.Index)
				}
				if position >= math.MaxInt64/EntryIndexBytes {
					input.Close()
					return nil, fmt.Errorf("WAL index exceeds supported file size")
				}
				if position < count {
					if err = writer.Flush(); err != nil {
						input.Close()
						return nil, err
					}
					if e.Index <= state.Commit {
						var oldBytes [EntryIndexBytes]byte
						if _, err = idx.ReadAt(oldBytes[:], int64(position)*EntryIndexBytes); err != nil {
							input.Close()
							return nil, err
						}
						old, err := p.readEntry(decodeIndex(oldBytes[:]), e.Index)
						if err != nil {
							input.Close()
							return nil, err
						}
						if old.Term != e.Term || old.Type != e.Type || !bytes.Equal(old.Data, e.Data) {
							input.Close()
							return nil, fmt.Errorf("WAL overwrites committed index %d", e.Index)
						}
					}
					if err = idx.Truncate(int64(position) * EntryIndexBytes); err != nil {
						input.Close()
						return nil, err
					}
					if _, err = idx.Seek(int64(position)*EntryIndexBytes, io.SeekStart); err != nil {
						input.Close()
						return nil, err
					}
				}
				t, err := anchors.cover(decoder.LastOffset())
				if err != nil {
					input.Close()
					return nil, fmt.Errorf("entry %d in %s: %w", e.Index, f.Name, err)
				}
				r := indexRecord{segment: uint32(fileNumber), previousCRC: seed, offset: uint64(offset), length: uint64(decoder.LastOffset() - offset), timeNS: t, term: e.Term, typeID: uint32(e.Type)}
				encoded := r.encode()
				if _, err = writer.Write(encoded[:]); err != nil {
					input.Close()
					return nil, err
				}
				count = position + 1
			case wal.SnapshotType:
				var snap walpb.Snapshot
				if err = snap.Unmarshal(rec.Data); err != nil {
					input.Close()
					return nil, err
				}
			default:
				input.Close()
				return nil, fmt.Errorf("unknown WAL record type %d", rec.Type)
			}
		}
		if err = input.Close(); err != nil {
			return nil, err
		}
		if first {
			return nil, fmt.Errorf("empty WAL segment %s", f.Name)
		}
		if decoder.LastOffset() != f.Size {
			return nil, fmt.Errorf("unvalidated trailing bytes in WAL segment %s", f.Name)
		}
		if hex.EncodeToString(hr.hash.Sum(nil)) != f.SHA256 {
			return nil, fmt.Errorf("WAL checksum mismatch: %s", f.Name)
		}
		previousCRC = decoder.LastCRC()
	}
	if state.Commit < m.Snapshot.Index {
		return nil, fmt.Errorf("WAL commit %d predates baseline index %d", state.Commit, m.Snapshot.Index)
	}
	finalCount := state.Commit - m.Snapshot.Index
	if finalCount > count {
		return nil, fmt.Errorf("WAL missing committed entries through index %d", state.Commit)
	}
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	p.Count = finalCount
	p.IndexBytes = int64(finalCount) * EntryIndexBytes
	if err = idx.Truncate(p.IndexBytes); err != nil {
		return nil, err
	}
	if err = idx.Sync(); err != nil {
		return nil, err
	}
	var md etcdserverpb.Metadata
	if err = md.Unmarshal(metadata); err != nil {
		return nil, fmt.Errorf("WAL metadata: %w", err)
	}
	if md.NodeID != m.MemberID || md.ClusterID != m.ClusterID {
		return nil, fmt.Errorf("WAL identity differs from manifest")
	}
	p.HardState = state
	ready, err := json.MarshalIndent(struct {
		Count         uint64 `json:"committed_entries"`
		EntryBytes    int64  `json:"entry_index_bytes"`
		AnchorBytes   int64  `json:"anchor_index_bytes"`
		CaptureDir    string `json:"capture_dir"`
		SourceVersion string `json:"source_version"`
	}{p.Count, p.IndexBytes, p.AnchorBytes, dir, m.EtcdVersion}, "", "  ")
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(filepath.Join(indexDir, "ready.json"), append(ready, '\n'), 0600); err != nil {
		return nil, err
	}
	return p, nil
}

type hashingReader struct {
	*os.File
	hash hash.Hash
	ctx  context.Context
}

func (r *hashingReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.File.Read(b)
	if n > 0 {
		_, _ = r.hash.Write(b[:n])
	}
	return n, err
}
func (r *hashingReader) FileInfo() (fs.FileInfo, error) { return r.Stat() }

func checkFrameBound(f *os.File, offset int64) error {
	var b [8]byte
	n, err := f.ReadAt(b[:], offset)
	if err == io.EOF && n == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	size := binary.LittleEndian.Uint64(b[:]) & ((uint64(1) << 56) - 1)
	if size > MaxRecordBytes {
		return fmt.Errorf("WAL record exceeds %d-byte limit", MaxRecordBytes)
	}
	return nil
}

type anchorRange struct{ start, count int64 }

func prepareAnchors(ctx context.Context, dir, indexDir string, start time.Time, files []WALFile, m *Manifest) ([]anchorRange, int64, error) {
	positions := make(map[string]int, len(files))
	for i, f := range files {
		positions[f.Name] = i
	}
	ranges := make([]anchorRange, len(files))
	in, err := os.Open(filepath.Join(dir, "anchors.jsonl"))
	if err != nil {
		return nil, 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(filepath.Join(indexDir, "anchors.idx"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, 0, err
	}
	defer out.Close()
	writer := bufio.NewWriterSize(out, 64<<10)
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var total, lastOffset int64
	lastFile := -1
	var lastTime time.Time
	warned := false
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			return nil, 0, err
		}
		var a Anchor
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		if err = json.Unmarshal(scanner.Bytes(), &a); err != nil {
			return nil, 0, err
		}
		file, ok := positions[a.Segment]
		if !ok || a.Offset <= 0 || a.Offset > files[file].Size || a.ObservedAt.IsZero() {
			return nil, 0, fmt.Errorf("invalid time anchor for %s at %d", a.Segment, a.Offset)
		}
		if file < lastFile {
			return nil, 0, fmt.Errorf("time anchor segment order regressed at %s", a.Segment)
		}
		if file != lastFile {
			ranges[file].start = total
			lastOffset = 0
		}
		if a.Offset <= lastOffset {
			return nil, 0, fmt.Errorf("time anchor offset did not advance for %s", a.Segment)
		}
		if !lastTime.IsZero() && a.ObservedAt.Before(lastTime) && !warned {
			m.Warnings = append(m.Warnings, "Observed wall clock moved backward; entry times are clamped monotonically")
			warned = true
		}
		t := a.ObservedAt.Sub(start)
		if t < 0 {
			t = 0
		}
		var b [AnchorIndexBytes]byte
		binary.LittleEndian.PutUint64(b[:8], uint64(a.Offset))
		binary.LittleEndian.PutUint64(b[8:], uint64(t))
		if _, err = writer.Write(b[:]); err != nil {
			return nil, 0, err
		}
		ranges[file].count++
		total += AnchorIndexBytes
		lastFile, lastOffset, lastTime = file, a.Offset, a.ObservedAt
	}
	if err = scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("read bounded anchor line: %w", err)
	}
	if err = writer.Flush(); err != nil {
		return nil, 0, err
	}
	if err = out.Sync(); err != nil {
		return nil, 0, err
	}
	return ranges, total, nil
}

type anchorCursor struct {
	reader            *bufio.Reader
	remaining         int64
	endOffset, timeNS int64
}

func newAnchorCursor(f *os.File, r anchorRange) *anchorCursor {
	return &anchorCursor{reader: bufio.NewReaderSize(io.NewSectionReader(f, r.start, r.count*AnchorIndexBytes), 32<<10), remaining: r.count}
}
func (c *anchorCursor) cover(offset int64) (int64, error) {
	for c.endOffset < offset {
		if c.remaining == 0 {
			return 0, fmt.Errorf("no time anchor covers offset %d", offset)
		}
		var b [AnchorIndexBytes]byte
		if _, err := io.ReadFull(c.reader, b[:]); err != nil {
			return 0, err
		}
		c.endOffset = int64(binary.LittleEndian.Uint64(b[:8]))
		c.timeNS = int64(binary.LittleEndian.Uint64(b[8:]))
		c.remaining--
	}
	return c.timeNS, nil
}

type EntryStream struct {
	prepared     *Prepared
	index        *os.File
	reader       *bufio.Reader
	source       *os.File
	segment      uint32
	position     uint64
	previousTime time.Duration
	closed       bool
}

func (p *Prepared) OpenStream() (*EntryStream, error) {
	f, err := os.Open(filepath.Join(p.IndexDir, "entries.idx"))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() != p.IndexBytes {
		f.Close()
		return nil, fmt.Errorf("entry index size changed")
	}
	return &EntryStream{prepared: p, index: f, reader: bufio.NewReaderSize(f, 64<<10)}, nil
}

func (s *EntryStream) Next(ctx context.Context) (raftpb.Entry, time.Duration, error) {
	var empty raftpb.Entry
	if err := ctx.Err(); err != nil {
		return empty, 0, err
	}
	if s.closed {
		return empty, 0, fmt.Errorf("entry stream closed")
	}
	if s.position >= s.prepared.Count {
		return empty, 0, io.EOF
	}
	var b [EntryIndexBytes]byte
	if _, err := io.ReadFull(s.reader, b[:]); err != nil {
		return empty, 0, err
	}
	r := decodeIndex(b[:])
	if int(r.segment) >= len(s.prepared.files) {
		return empty, 0, fmt.Errorf("invalid entry segment index")
	}
	if s.source == nil || s.segment != r.segment {
		if s.source != nil {
			_ = s.source.Close()
		}
		f, err := os.Open(filepath.Join(s.prepared.captureDir, "wal", s.prepared.files[r.segment].Name))
		if err != nil {
			return empty, 0, err
		}
		s.source, s.segment = f, r.segment
	}
	e, err := readIndexedEntry(s.source, r, s.prepared.Manifest.Snapshot.Index+s.position+1)
	if err != nil {
		return empty, 0, err
	}
	t := time.Duration(r.timeNS)
	if t < s.previousTime {
		t = s.previousTime
	}
	s.previousTime = t
	s.position++
	return e, t, nil
}

func (s *EntryStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.source != nil {
		err = s.source.Close()
	}
	if e := s.index.Close(); err == nil {
		err = e
	}
	return err
}

func (p *Prepared) readEntry(r indexRecord, index uint64) (raftpb.Entry, error) {
	if int(r.segment) >= len(p.files) {
		return raftpb.Entry{}, fmt.Errorf("invalid indexed segment")
	}
	f, err := os.Open(filepath.Join(p.captureDir, "wal", p.files[r.segment].Name))
	if err != nil {
		return raftpb.Entry{}, err
	}
	defer f.Close()
	return readIndexedEntry(f, r, index)
}

func readIndexedEntry(f *os.File, r indexRecord, index uint64) (raftpb.Entry, error) {
	var e raftpb.Entry
	if r.length < 8 || r.length > MaxRecordBytes+15 || r.offset > math.MaxInt64 {
		return e, fmt.Errorf("invalid indexed frame bounds")
	}
	frame := make([]byte, int(r.length))
	if _, err := f.ReadAt(frame, int64(r.offset)); err != nil {
		return e, err
	}
	header := binary.LittleEndian.Uint64(frame[:8])
	size := header & ((uint64(1) << 56) - 1)
	padding := uint64(0)
	if header>>63 != 0 {
		padding = (header >> 56) & 7
	}
	if 8+size+padding != r.length {
		return e, fmt.Errorf("indexed WAL frame length changed")
	}
	var rec walpb.Record
	if err := rec.Unmarshal(frame[8 : 8+size]); err != nil {
		return e, err
	}
	if rec.Type != wal.EntryType {
		return e, fmt.Errorf("indexed WAL record is not an entry")
	}
	crc := crc32.Update(r.previousCRC, crc32.MakeTable(crc32.Castagnoli), rec.Data)
	if err := rec.Validate(crc); err != nil {
		return e, fmt.Errorf("indexed WAL record CRC: %w", err)
	}
	if err := e.Unmarshal(rec.Data); err != nil {
		return e, err
	}
	if e.Index != index || e.Term != r.term || uint32(e.Type) != r.typeID {
		return e, fmt.Errorf("indexed WAL entry identity changed")
	}
	return e, nil
}
