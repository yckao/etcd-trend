package etcdutil

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// KVDigester computes the canonical logical hash in key order using memory
// bounded by the largest key/value. It frames each canonical protobuf with an
// unsigned 64-bit big-endian length, including revision, version and lease data.
// The input must contain each current key exactly once in ascending byte order.
type KVDigester struct {
	h        hash.Hash
	previous []byte
	buffer   []byte
	count    int
}

func NewKVDigester() *KVDigester { return &KVDigester{h: sha256.New()} }

func (d *KVDigester) Add(kv *mvccpb.KeyValue) error {
	if d.count > 0 && bytes.Compare(d.previous, kv.Key) >= 0 {
		return fmt.Errorf("logical digest keys are not strictly increasing")
	}
	size := kv.Size()
	if cap(d.buffer) < size {
		d.buffer = make([]byte, size)
	} else {
		d.buffer = d.buffer[:size]
	}
	n, err := kv.MarshalTo(d.buffer)
	if err != nil {
		return err
	}
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], uint64(n))
	_, _ = d.h.Write(prefix[:])
	_, _ = d.h.Write(d.buffer[:n])
	d.previous = append(d.previous[:0], kv.Key...)
	d.count++
	return nil
}
func (d *KVDigester) Sum() string { return hex.EncodeToString(d.h.Sum(nil)) }
func (d *KVDigester) Count() int  { return d.count }
