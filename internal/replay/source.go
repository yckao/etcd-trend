package replay

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// EntrySource yields canonical committed entries after the baseline, in index
// order. The caller owns its lifetime and must close it when Run returns.
type EntrySource interface {
	Next(context.Context) (raftpb.Entry, time.Duration, error)
}

type sliceSource struct {
	entries  []raftpb.Entry
	times    map[uint64]time.Duration
	position int
}

func (s *sliceSource) Next(ctx context.Context) (raftpb.Entry, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return raftpb.Entry{}, 0, err
	}
	if s.position == len(s.entries) {
		return raftpb.Entry{}, 0, io.EOF
	}
	e := s.entries[s.position]
	s.position++
	t, ok := s.times[e.Index]
	if !ok {
		return e, 0, fmt.Errorf("missing time anchor for entry %d", e.Index)
	}
	return e, t, nil
}
