package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal"
	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/wal/walpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/raft/v3/raftpb"
)

var walNamePattern = regexp.MustCompile(`^([0-9a-f]{16})-([0-9a-f]{16})\.wal$`)

func ParseWALName(name string) (sequence, index uint64, err error) {
	m := walNamePattern.FindStringSubmatch(name)
	if m == nil {
		return 0, 0, fmt.Errorf("invalid WAL segment name %q", name)
	}
	sequence, err = strconv.ParseUint(m[1], 16, 64)
	if err != nil {
		return 0, 0, err
	}
	index, err = strconv.ParseUint(m[2], 16, 64)
	return
}

// Load is an eager compatibility helper for small test fixtures. Production
// analyzers must use Prepare/OpenStream, which do not retain entries or anchors
// in memory. It never opens or contacts the recording source.
func Load(dir string) (*Dataset, error) {
	m, err := ReadManifest(dir)
	if err != nil {
		return nil, err
	}
	if m.State != "complete" {
		return nil, fmt.Errorf("capture state is %q: %s", m.State, m.Error)
	}
	if m.Snapshot.File != "baseline.db" {
		return nil, fmt.Errorf("unsupported snapshot path %q", m.Snapshot.File)
	}
	d := &Dataset{Manifest: m, BaselinePath: filepath.Join(dir, m.Snapshot.File), EntryTimes: map[uint64]time.Duration{}}
	snapshot, err := InspectSnapshot(d.BaselinePath)
	if err != nil {
		return nil, err
	}
	if snapshot.SHA256 != m.Snapshot.SHA256 || snapshot.Size != m.Snapshot.Size || snapshot.Index != m.Snapshot.Index || snapshot.Term != m.Snapshot.Term || snapshot.Revision != m.Snapshot.Revision {
		return nil, fmt.Errorf("baseline differs from capture manifest")
	}
	d.Anchors, err = readAnchors(filepath.Join(dir, "anchors.jsonl"))
	if err != nil {
		return nil, err
	}
	anchorByFile := map[string][]Anchor{}
	fileSizes := map[string]int64{}
	for _, f := range m.WALFiles {
		fileSizes[f.Name] = f.Size
	}
	for _, a := range d.Anchors {
		if size, ok := fileSizes[a.Segment]; !ok || a.Offset <= 0 || a.Offset > size || a.ObservedAt.IsZero() {
			return nil, fmt.Errorf("invalid time anchor for %s at %d", a.Segment, a.Offset)
		}
		prior := anchorByFile[a.Segment]
		if len(prior) > 0 {
			last := prior[len(prior)-1]
			if a.Offset <= last.Offset {
				return nil, fmt.Errorf("time anchor offset did not advance for %s", a.Segment)
			}
			if a.ObservedAt.Before(last.ObservedAt) {
				d.Manifest.Warnings = append(d.Manifest.Warnings, "Observed wall clock moved backward; entry times are clamped monotonically")
			}
		}
		anchorByFile[a.Segment] = append(anchorByFile[a.Segment], a)
	}
	files := append([]WALFile(nil), m.WALFiles...)
	sort.Slice(files, func(i, j int) bool { return files[i].Sequence < files[j].Sequence })
	if len(files) == 0 {
		return nil, fmt.Errorf("capture contains no WAL segments")
	}
	var crc uint32
	var metadata []byte
	var state raftpb.HardState
	var entryTimes []time.Duration
	for i, f := range files {
		seq, _, err := ParseWALName(f.Name)
		if err != nil {
			return nil, err
		}
		if seq != f.Sequence || (i > 0 && seq != files[i-1].Sequence+1) {
			return nil, fmt.Errorf("WAL sequence gap or invalid manifest at %s", f.Name)
		}
		path := filepath.Join(dir, "wal", f.Name)
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() != f.Size {
			return nil, fmt.Errorf("WAL size mismatch: %s", f.Name)
		}
		hash, err := FileSHA256(path)
		if err != nil {
			return nil, err
		}
		if hash != f.SHA256 {
			return nil, fmt.Errorf("WAL checksum mismatch: %s", f.Name)
		}
		input, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		decoder := wal.NewDecoder(fileutil.NewFileReader(input))
		decoder.UpdateCRC(crc)
		first := true
		for {
			var rec walpb.Record
			err = decoder.Decode(&rec)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				input.Close()
				return nil, fmt.Errorf("decode %s at %d: %w", f.Name, decoder.LastOffset(), err)
			}
			if first && rec.Type != wal.CrcType {
				input.Close()
				return nil, fmt.Errorf("missing initial CRC record: %s", f.Name)
			}
			first = false
			switch rec.Type {
			case wal.CrcType:
				if i > 0 && rec.Crc != decoder.LastCRC() {
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
				offset := e.Index - m.Snapshot.Index - 1
				if offset > uint64(len(d.Entries)) {
					input.Close()
					return nil, fmt.Errorf("WAL entry gap: expected index %d, found %d", m.Snapshot.Index+uint64(len(d.Entries))+1, e.Index)
				}
				if offset < uint64(len(d.Entries)) && e.Index <= state.Commit {
					old := d.Entries[offset]
					if old.Term != e.Term || old.Type != e.Type || !bytes.Equal(old.Data, e.Data) {
						input.Close()
						return nil, fmt.Errorf("WAL overwrites committed index %d", e.Index)
					}
				}
				d.Entries = append(d.Entries[:offset], e)
				t, err := anchorTime(anchorByFile[f.Name], decoder.LastOffset(), m.StartedAt)
				if err != nil {
					input.Close()
					return nil, fmt.Errorf("entry %d in %s: %w", e.Index, f.Name, err)
				}
				entryTimes = append(entryTimes[:offset], t)
			case wal.SnapshotType:
				var s walpb.Snapshot
				if err = s.Unmarshal(rec.Data); err != nil {
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
		crc = decoder.LastCRC()
	}
	if state.Commit < m.Snapshot.Index {
		return nil, fmt.Errorf("WAL commit %d predates baseline index %d", state.Commit, m.Snapshot.Index)
	}
	n := state.Commit - m.Snapshot.Index
	if n > uint64(len(d.Entries)) {
		return nil, fmt.Errorf("WAL missing committed entries through index %d", state.Commit)
	}
	d.Entries = d.Entries[:n]
	var prev time.Duration
	for i, e := range d.Entries {
		t := entryTimes[i]
		if t < prev {
			t = prev
		}
		d.EntryTimes[e.Index] = t
		prev = t
	}
	d.HardState = state
	var md etcdserverpb.Metadata
	if err = md.Unmarshal(metadata); err != nil {
		return nil, fmt.Errorf("WAL metadata: %w", err)
	}
	if md.NodeID != m.MemberID || md.ClusterID != m.ClusterID {
		return nil, fmt.Errorf("WAL identity differs from manifest")
	}
	return d, nil
}

func readAnchors(path string) ([]Anchor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var anchors []Anchor
	d := json.NewDecoder(f)
	for {
		var a Anchor
		if err := d.Decode(&a); errors.Is(err, io.EOF) {
			return anchors, nil
		} else if err != nil {
			return nil, err
		}
		anchors = append(anchors, a)
	}
}

func anchorTime(anchors []Anchor, offset int64, start time.Time) (time.Duration, error) {
	for _, a := range anchors {
		if a.Offset >= offset {
			t := a.ObservedAt.Sub(start)
			if t < 0 {
				return 0, nil
			}
			return t, nil
		}
	}
	return 0, fmt.Errorf("no time anchor covers offset %d", offset)
}
