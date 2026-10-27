package capture

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

// InspectSnapshot must only be called on the recorder's immutable copy. Opening
// the live bbolt file, even read-only, can contend on etcd's file lock.
func InspectSnapshot(path string) (Snapshot, error) {
	var s Snapshot
	info, err := os.Stat(path)
	if err != nil {
		return s, err
	}
	if !info.Mode().IsRegular() {
		return s, fmt.Errorf("snapshot is not a regular file")
	}
	s.Size = info.Size()
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return s, fmt.Errorf("open snapshot: %w", err)
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		keys := tx.Bucket([]byte("key"))
		if meta == nil || keys == nil {
			return fmt.Errorf("snapshot is not an etcd v3 backend")
		}
		index := meta.Get([]byte("consistent_index"))
		if len(index) != 8 {
			return fmt.Errorf("snapshot has no valid consistent_index")
		}
		s.Index = binary.BigEndian.Uint64(index)
		if term := meta.Get([]byte("term")); len(term) == 8 {
			s.Term = binary.BigEndian.Uint64(term)
		}
		s.Revision = 1
		if k, _ := keys.Cursor().Last(); len(k) >= 8 {
			s.Revision = int64(binary.BigEndian.Uint64(k[:8]))
		}
		for _, key := range []string{"finishedCompactRev", "scheduledCompactRev"} {
			if rev := meta.Get([]byte(key)); len(rev) >= 8 {
				if r := int64(binary.BigEndian.Uint64(rev[:8])); r > s.Revision {
					s.Revision = r
				}
			}
		}
		var checkErr error
		for err := range tx.Check() {
			if checkErr == nil {
				checkErr = fmt.Errorf("snapshot consistency check: %w", err)
			}
		}
		return checkErr
	})
	if err != nil {
		return s, err
	}
	s.SHA256, err = FileSHA256(path)
	return s, err
}
