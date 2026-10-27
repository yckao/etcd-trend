package etcdutil

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	"testing"
)

func TestDigestFramingAndOrder(t *testing.T) {
	kvs := []mvccpb.KeyValue{{Key: []byte("a"), Value: []byte("bc"), CreateRevision: 1, ModRevision: 2, Version: 2, Lease: 9}, {Key: []byte("a\x00"), Value: []byte("d"), ModRevision: 3}}
	d := NewKVDigester()
	for i := range kvs {
		if err := d.Add(&kvs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if d.Sum() != HashKVs(kvs) || d.Count() != 2 {
		t.Fatal("incremental digest mismatch")
	}
	if err := d.Add(&kvs[1]); err == nil {
		t.Fatal("duplicate key accepted")
	}
	changed := append([]mvccpb.KeyValue(nil), kvs...)
	changed[0].Lease++
	if HashKVs(changed) == d.Sum() {
		t.Fatal("digest ignored lease metadata")
	}
}
func TestSourceVersions(t *testing.T) {
	for _, s := range []string{"3.4.0", "3.4.45", "3.5.34", "3.6.0", "v3.6.3", "3.6.2+vendor.1"} {
		if _, err := ParseSourceVersion(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	for _, s := range []string{"", "3.4", "3.3.99", "3.7.0", "4.0.0", "3.6.0-rc.1", "3.6.00", "3.6.1+", "3.6.1\n", "3.6.999999999999999999999999999999"} {
		if _, err := ParseSourceVersion(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}
