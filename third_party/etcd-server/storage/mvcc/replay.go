// Copyright 2026 etcd-trend contributors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"context"
	"fmt"

	"github.com/yckao/etcd-trend/third_party/etcd-server/storage/schema"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// ReplayVisitCurrent streams the current logical keyspace once in byte-key
// order. It is only for a quiescent offline replay store after its final flush.
// A normal limited Range also computes the total count of the remaining range;
// repeatedly paging that API would perform quadratic index traversal. This
// iterator instead visits each index key once and retains one decoded value.
// The visitor must not call the store or backend, and must copy retained bytes.
func ReplayVisitCurrent(ctx context.Context, kv KV, visit func(mvccpb.KeyValue) error) error {
	s, ok := kv.(*store)
	if !ok {
		return fmt.Errorf("ReplayVisitCurrent requires an offline MVCC store")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.revMu.RLock()
	rev := s.currentRev
	s.revMu.RUnlock()
	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	ti, ok := s.kvindex.(*treeIndex)
	if !ok {
		return fmt.Errorf("unsupported replay index implementation")
	}
	ti.RLock()
	defer ti.RUnlock()
	var result error
	keybuf := NewRevBytes()
	ti.unsafeVisit([]byte{}, []byte{}, func(ki *keyIndex) bool {
		if err := ctx.Err(); err != nil {
			result = err
			return false
		}
		current, _, _, err := ki.get(s.lg, rev)
		if err == ErrRevisionNotFound {
			return true
		}
		if err != nil {
			result = err
			return false
		}
		keybuf = RevToBytes(current, keybuf)
		_, values := tx.UnsafeRange(schema.Key, keybuf, nil, 0)
		if len(values) != 1 {
			result = fmt.Errorf("missing backend value for current revision %v", current)
			return false
		}
		var value mvccpb.KeyValue
		if err = value.Unmarshal(values[0]); err != nil {
			result = err
			return false
		}
		if err = visit(value); err != nil {
			result = err
			return false
		}
		return true
	})
	return result
}
