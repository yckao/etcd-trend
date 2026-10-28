# Offline replay changes

This directory contains `go.etcd.io/etcd/server/v3` v3.6.0, module checksum
`h1:YcYxiJzmFCpjzzd7d/XmQE09p60248OzaaOaySRJyt0=`, upstream commit
`f5d605a93abe2fdfa13880aab0f1f3a4203628dd`.

The upstream license and source headers are retained. The changes below are
opt-in; normal etcd defaults do not activate the replay behavior.

## Module packaging

The patched server sources are part of the `github.com/yckao/etcd-trend` module,
under `github.com/yckao/etcd-trend/third_party/etcd-server`. Go import paths for
this source tree and its callers use that prefix. The upstream module files are
preserved as `go.mod.upstream.txt` and `go.sum.upstream.txt` for provenance; they
are not active module boundaries. The root module has no replacement directive
or dependency on the upstream server module. Consequently, versioned
`go install github.com/yckao/etcd-trend@VERSION` includes the
patched engine in one downloadable module without resolving a local replace.

This is a packaging change. It preserves the upstream protobuf schemas,
registration names, wire formats, licenses, tests, and replay behavior. The
original protobuf `go_package` metadata is retained with the upstream schemas;
compiled Go imports are the project-local paths described above.

The server module archive did not contain the sibling repository fixtures used
by its JWT tests. Six unchanged public test certificates/keys from the same
upstream commit are restored under `testdata/fixtures`, with hashes and source
URLs in `provenance.json`. `auth/jwt_test.go` uses that package-relative
location, including when tests run from a downloaded module. No upstream tests
are excluded by build tags or package selection.

`embed/config_test.go` checks the invalid log-size configuration using
`json.UnmarshalTypeError` fields rather than an exact diagnostic string: Go
1.26 adds the embedded `Logger` name to the reported JSON field path. The same
invalid configuration remains rejected; production code is unchanged.

## Replay behavior

- `storage/backend/backend.go`: optional manual commit scheduling and completed
  commit observer. The default wall-clock timer is disabled only in replay mode.
- `storage/backend/replay.go`: pending-work timer tick and commit observations
  without opening read transactions or adding commits for observation.
- `storage/backend/batch_tx.go`: observation after real bbolt commit; synchronous
  release of the prior read transaction in replay mode, removing an otherwise
  wall-clock-dependent page reclamation race in the offline model.
- `storage/mvcc/kvstore.go`: optional synchronous physical compaction, including
  recovery of unfinished compaction and already-compacted barriers.
- `storage/mvcc/replay.go`: linear-time final keyspace iterator, retaining one
  decoded value at a time. It is used only after replay writes have finished.
- `storage/mvcc/replay_mutation.go`, `kvstore_txn.go`, and
  `kvstore_compaction.go`: optional observation of actual MVCC puts, tombstones,
  and physically removed records. Events include key identity and serialized
  sizes and a callback-borrowed value for offline Kubernetes metadata extraction.
  The value is excluded from JSON and must not be retained by observers.
  A separate read-only iterator scans all retained versions
  in an immutable offline baseline. Observers do not open backend readers, force
  commits, or change the underlying operations.
- `etcdserver/api/membership/cluster.go`: opt-in offline recovery skips serving
  member upgrade/downgrade gates. Source version remains recorded; all physical
  simulation still uses the pinned 3.6.0 engine.
- `storage/mvcc/kvstore_compaction.go`: optional virtual batch sleep callback
  with cancellation propagation.

The first replay model executes physical compaction as a serial barrier. It
advances virtual time between batches and executes pending backend timer ticks,
but does not interleave future WAL entries during cleanup. This is a declared
simulation policy, not a claim to reproduce a member's original scheduling.

The analyzer must not be used as a replacement etcd server. It has no serving,
Raft networking, election, or autonomous lease expiration path. The production
etcd process is never patched by this project.
