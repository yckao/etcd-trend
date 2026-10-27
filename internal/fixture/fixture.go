// Package fixture creates an isolated etcd member for end-to-end qualification.
// It is used only by tests and the demo, never by recorder or analyzer.
package fixture

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

type Instance struct {
	BaseDB string
	WALDir string
	Client *clientv3.Client
	server *embed.Etcd
}

type Workload struct {
	Updates    int
	ValueBytes int
	Interval   time.Duration
}

type Expected struct {
	Revision          int64              `json:"revision"`
	CompactedRevision int64              `json:"compacted_revision"`
	KVs               []*mvccpb.KeyValue `json:"-"`
	Updates           int                `json:"updates"`
	ValueBytes        int                `json:"value_bytes"`
}

// Start creates a private loopback member, seeds it, takes an offline baseline,
// and restarts it. The temporary shutdown belongs only to this synthetic fixture.
func Start(ctx context.Context, dir string) (*Instance, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	cfg := embed.NewConfig()
	cfg.Name = "etcd-trend-fixture"
	cfg.Dir = filepath.Join(dir, "member")
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(dir, "etcd.log")}
	cfg.TickMs = 10
	cfg.ElectionMs = 100
	cfg.SnapshotCount = 50
	cfg.MaxWalFiles = 0
	cfg.BackendBatchInterval = 100 * time.Millisecond
	cfg.BackendBatchLimit = 10000
	cfg.CompactionBatchLimit = 16
	cfg.CompactionSleepInterval = time.Millisecond
	peer, err := freeURL()
	if err != nil {
		return nil, err
	}
	client, err := freeURL()
	if err != nil {
		return nil, err
	}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.ListenClientUrls = []url.URL{client}
	cfg.AdvertiseClientUrls = []url.URL{client}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := launch(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c, err := connect(client.String())
	if err != nil {
		server.Close()
		return nil, err
	}
	seed, err := c.Txn(ctx).Then(
		clientv3.OpPut("/registry/configmaps/default/stable", "baseline"),
		clientv3.OpPut("/registry/widgets/default/hot", "initial"),
		clientv3.OpPut("/registry/pods/default/delete-me", "old-pod"),
	).Commit()
	_ = seed
	c.Close()
	server.Close()
	if err != nil {
		return nil, err
	}
	base := filepath.Join(dir, "baseline.db")
	if err := copyFile(filepath.Join(cfg.Dir, "member", "snap", "db"), base); err != nil {
		return nil, err
	}
	server, err = launch(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c, err = connect(client.String())
	if err != nil {
		server.Close()
		return nil, err
	}
	return &Instance{BaseDB: base, WALDir: filepath.Join(cfg.Dir, "member", "wal"), Client: c, server: server}, nil
}

func (i *Instance) Close() {
	if i.Client != nil {
		i.Client.Close()
		i.Client = nil
	}
	if i.server != nil {
		i.server.Close()
		i.server = nil
	}
}

// Exercise contains repeated large values, both Txn branches, range deletes,
// lease revocation, and physical compaction. All RPCs target only the fixture.
func (i *Instance) Exercise(ctx context.Context, w Workload) (*Expected, error) {
	if w.Updates <= 0 {
		w.Updates = 96
	}
	if w.ValueBytes < 64 {
		w.ValueBytes = 12 * 1024
	}
	var lastRev int64
	for n := 0; n < w.Updates; n++ {
		value := fmt.Sprintf("%016d", n) + strings.Repeat(string(rune('a'+n%26)), w.ValueBytes-16)
		r, err := i.Client.Txn(ctx).
			If(clientv3.Compare(clientv3.Value("/registry/configmaps/default/stable"), "=", "baseline")).
			Then(clientv3.OpPut("/registry/widgets/default/hot", value), clientv3.OpPut(fmt.Sprintf("/registry/events/default/e-%03d", n), fmt.Sprint(n))).
			Else(clientv3.OpPut("/registry/unexpected", "wrong branch")).Commit()
		if err != nil {
			return nil, err
		}
		lastRev = r.Header.Revision
		if w.Interval > 0 {
			select {
			case <-time.After(w.Interval):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	// Exercise the failure branch and a successful revision compare.
	r, err := i.Client.Txn(ctx).If(clientv3.Compare(clientv3.Version("/registry/missing"), ">", 0)).
		Then(clientv3.OpPut("/registry/unexpected", "wrong branch")).
		Else(clientv3.OpPut("/registry/configmaps/default/branch", "failure branch")).Commit()
	if err != nil {
		return nil, err
	}
	r, err = i.Client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("/registry/configmaps/default/branch"), "=", r.Header.Revision)).
		Then(clientv3.OpPut("/registry/configmaps/default/branch", "revision checked")).Commit()
	if err != nil {
		return nil, err
	}
	lease, err := i.Client.Grant(ctx, 3600)
	if err != nil {
		return nil, err
	}
	if _, err = i.Client.Put(ctx, "/registry/leases/default/transient", "leased", clientv3.WithLease(lease.ID)); err != nil {
		return nil, err
	}
	if _, err = i.Client.Revoke(ctx, lease.ID); err != nil {
		return nil, err
	}
	// Retain a lease as well, to verify lease IDs and associations survive replay.
	retained, err := i.Client.Grant(ctx, 3600)
	if err != nil {
		return nil, err
	}
	if _, err = i.Client.Put(ctx, "/registry/leases/default/retained", "live lease", clientv3.WithLease(retained.ID)); err != nil {
		return nil, err
	}
	if _, err = i.Client.Delete(ctx, "/registry/events/", clientv3.WithPrefix()); err != nil {
		return nil, err
	}
	if _, err = i.Client.Delete(ctx, "/registry/pods/default/delete-me"); err != nil {
		return nil, err
	}
	compactRev := lastRev - int64(w.Updates/3)
	if _, err = i.Client.Compact(ctx, compactRev, clientv3.WithCompactPhysical()); err != nil {
		return nil, err
	}
	if _, err = i.Client.Put(ctx, "/registry/configmaps/default/final", "done"); err != nil {
		return nil, err
	}
	response, err := i.Client.Get(ctx, "", clientv3.WithFromKey())
	if err != nil {
		return nil, err
	}
	return &Expected{Revision: response.Header.Revision, CompactedRevision: compactRev, KVs: response.Kvs, Updates: w.Updates, ValueBytes: w.ValueBytes}, nil
}

func launch(ctx context.Context, cfg *embed.Config) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}
	select {
	case <-e.Server.ReadyNotify():
		return e, nil
	case <-ctx.Done():
		e.Close()
		return nil, ctx.Err()
	case <-time.After(15 * time.Second):
		e.Close()
		return nil, fmt.Errorf("fixture etcd readiness timed out")
	}
}

func connect(endpoint string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
}

func freeURL() (url.URL, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return url.URL{}, err
	}
	addr := l.Addr().String()
	l.Close()
	u, err := url.Parse("http://" + addr)
	if err != nil {
		return url.URL{}, err
	}
	return *u, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
