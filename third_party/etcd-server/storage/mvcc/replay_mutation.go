// Copyright 2026 etcd-trend contributors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/schema"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// ReplayMutation describes one actual MVCC backend record. Its transient value
// supports offline metadata extraction. Kind is baseline, put, delete, or compact. RecordBytes is the serialized
// KeyValue plus its revision bucket key; it excludes bbolt page/element overhead.
// Revision and SubRevision always come from the backend key, including tombstones
// whose serialized KeyValue carries no revision. Key and Value are borrowed for
// the callback duration; analysis must not retain or persist Value.
type ReplayMutation struct {
	Kind        string
	Key         []byte
	Value       []byte `json:"-"`
	Revision    int64
	SubRevision int64
	ValueBytes  int64
	RecordBytes int64
	Tombstone   bool
}

func replayMutationFromRecord(kind string, key, data []byte) (ReplayMutation, error) {
	var event ReplayMutation
	if (len(key) != revBytesLen && len(key) != markedRevBytesLen) || key[8] != '_' || (len(key) == markedRevBytesLen && key[markBytePosition] != markTombstone) {
		return event, fmt.Errorf("invalid MVCC revision key in %s observation", kind)
	}
	main, sub := int64(binary.BigEndian.Uint64(key[:8])), int64(binary.BigEndian.Uint64(key[9:17]))
	if main < 0 || sub < 0 {
		return event, fmt.Errorf("negative MVCC revision in %s observation", kind)
	}
	var kv mvccpb.KeyValue
	if err := kv.Unmarshal(data); err != nil {
		return event, fmt.Errorf("decode MVCC %s record at %d.%d: %w", kind, main, sub, err)
	}
	return ReplayMutation{Kind: kind, Key: kv.Key, Value: kv.Value, Revision: main, SubRevision: sub, ValueBytes: int64(len(kv.Value)), RecordBytes: int64(len(key) + len(data)), Tombstone: isTombstone(key)}, nil
}

// ReplayVisitRecords scans every retained MVCC record in revision order from an
// immutable offline backend image. It includes obsolete versions and tombstones,
// unlike ReplayVisitCurrent. It retains only one decoded record at a time. The
// image must not be a live member's mutable backend or the active replay backend.
func ReplayVisitRecords(ctx context.Context, path string, visit func(ReplayMutation) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if visit == nil {
		return fmt.Errorf("MVCC record visitor is required")
	}
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open offline MVCC baseline: %w", err)
	}
	defer db.Close()
	return db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(schema.Key.Name())
		if bucket == nil {
			return fmt.Errorf("offline baseline has no MVCC key bucket")
		}
		cursor := bucket.Cursor()
		for key, data := cursor.First(); key != nil; key, data = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			event, err := replayMutationFromRecord("baseline", key, data)
			if err != nil {
				return err
			}
			if err := visit(event); err != nil {
				return err
			}
		}
		return nil
	})
}
