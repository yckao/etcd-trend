// Package etcdutil contains offline helpers for etcd database copies.
package etcdutil

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// DBInfo describes a consistent offline backend copy, not a running database.
type DBInfo struct {
	Index    uint64 `json:"index"`
	Term     uint64 `json:"term"`
	Revision int64  `json:"revision"`
	TxID     int    `json:"txid"`
	DBSize   int64  `json:"db_size"`
}

// InspectDB opens a supplied immutable copy read-only. Callers must not point
// this at a running member: bbolt file locking would conflict with that member.
func InspectDB(path string) (info DBInfo, err error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return info, err
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		info.TxID = tx.ID()
		info.DBSize = tx.Size()
		meta := tx.Bucket([]byte("meta"))
		if meta == nil {
			return fmt.Errorf("missing etcd meta bucket")
		}
		for name, dest := range map[string]*uint64{"consistent_index": &info.Index, "term": &info.Term} {
			v := meta.Get([]byte(name))
			if len(v) == 8 {
				*dest = binary.BigEndian.Uint64(v)
			}
		}
		info.Revision = 1
		keys := tx.Bucket([]byte("key"))
		if keys == nil {
			return fmt.Errorf("missing etcd key bucket")
		}
		k, _ := keys.Cursor().Last()
		if len(k) >= 8 {
			info.Revision = int64(binary.BigEndian.Uint64(k[:8]))
		}
		for _, name := range []string{"finishedCompactRev", "scheduledCompactRev"} {
			if v := meta.Get([]byte(name)); len(v) >= 8 {
				if rev := int64(binary.BigEndian.Uint64(v[:8])); rev > info.Revision {
					info.Revision = rev
				}
			}
		}
		return nil
	})
	return info, err
}

// HashKVs hashes sorted complete logical KVs, including all revision and lease
// metadata. Length framing prevents concatenation collisions.
func HashKVs(kvs []mvccpb.KeyValue) string {
	sorted := append([]mvccpb.KeyValue(nil), kvs...)
	sort.Slice(sorted, func(i, j int) bool { return string(sorted[i].Key) < string(sorted[j].Key) })
	d := NewKVDigester()
	for i := range sorted {
		if err := d.Add(&sorted[i]); err != nil {
			panic(err)
		}
	}
	return d.Sum()
}
