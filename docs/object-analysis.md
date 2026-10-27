# Object and resource analysis

Object analysis follows the actual MVCC mutations made during offline replay. It reports which storage keys and Kubernetes storage prefixes contribute most to retained history, write activity, database growth, and reusable free-space changes in each virtual-time window. It uses only the captured baseline and WAL.

## Run and inspect

```sh
bin/analyzer --capture captures/session --out runs/analyzed \
  --speed max --window 1m --top 20

bin/etcd-trend-inspect --run runs/analyzed --window 0 --metric history
bin/etcd-trend-inspect --run runs/analyzed --window 0 --metric db-growth
bin/etcd-trend-inspect --run runs/analyzed --window 0 --metric free-net
```

Object analysis is enabled by default in the analyzer CLI. `--object-analysis=false` runs the storage simulation without attribution. Library callers enable it explicitly with `replay.Config.AnalyzeObjects`. `--window` uses virtual source time, independent of playback speed. Window numbers in the inspector start at zero. `--scope object` or `--scope resource` limits the displayed ranking; the default displays both. `--limit` cannot recover ranks discarded by replay's `--top` setting; all contributor rows remain in JSONL.

The inspector's metrics are:

| Metric | Ranking quantity | Meaning |
| --- | --- | --- |
| `writes` | Value bytes actually written | Workload intensity, including repeated updates. |
| `history` | Positive net historical-record byte growth | Additional retained old versions and tombstones during the window. |
| `retained` | Positive net retained MVCC byte growth | Growth of all retained records, including current values. |
| `compacted` | MVCC record bytes actually removed | Logical history reclaimed by physical compaction. |
| `db-growth` | Estimated share of observed DB growth | Commit-level association with mutated record bytes. |
| `free-growth` | Estimated share of gross free-space increases | Compaction-associated creation of reusable space. |
| `free-net` | Positive estimated free growth minus free reuse | Estimated contribution remaining after attributed reuse in the same window. |

Read `history` and `retained` to explain stored data growth, then `db-growth` and the physical endpoints to assess the modeled file-space effect. Read `compacted`, `free-growth`, and `free-net` together to distinguish reclaimed history from reusable space that remains at the end of a window. A large write volume is not automatically an equally large fragmentation contribution.

## Exact measurements

For each key, the baseline scan accounts for every retained MVCC version and tombstone in revision order. Baseline bytes are starting inventory, not replay write traffic. A large object that remains unchanged therefore has a large initial stock and zero write contribution.

Hooks run after each real MVCC record write or removal and before the backend can commit it. They cover the executed Txn branch, nested Txn writes, every key in range deletes, lease-revocation deletes, and each record physically removed by compaction. Failed comparisons do not contribute writes from the unexecuted branch. Lease timing remains owned by captured WAL operations.

Each hook records the key, source index, revision/subrevision, value length, and record length. The callback also borrows the value while extracting selected Kubernetes metadata; raw values are neither retained nor emitted by attribution. Record bytes mean the serialized `mvccpb.KeyValue` plus its revision bucket key; they exclude bbolt page headers, element overhead, and page slack.

For each object and resource:

```text
retained MVCC bytes = sum of all currently retained record bytes
live record bytes  = latest non-tombstone record bytes
history bytes      = retained MVCC bytes - live record bytes
```

History includes superseded versions and tombstones. A delete creates a tombstone and moves the previous live record into history; compaction later removes actual records. The analyzer does not assume that every record at or below a requested compaction revision has been removed. Inventory object counts include deleted keys with retained records; `Stock.LiveObjects` is the count of currently live keys.

Other exact counters include puts, deleted keys, value/record bytes written, superseded live-record bytes, compacted records/bytes, live object count, and live value bytes. The first/last contributing Raft indexes in each changed row support tracing a window back to its operation timeline.

## Physical association model

Physical observations are actual completed bbolt commits in the selected offline simulation. The global measurements include DB size, in-use size, free-page count, and pending-page count. Reusable free bytes use the existing backend metric definition:

```text
free bytes = DB size - DB size in use
```

This metric does not measure unused bytes inside allocated B+tree pages. Pending-page counts are reported separately. The physical engine is still the pinned etcd 3.6.0 model with serial compaction and the selected batching parameters.

For object `i` in commit `c`, the default allocation weights are:

```text
changed(i,c) = MVCC record bytes written + MVCC record bytes compacted
released(i,c) = MVCC record bytes compacted in the immediately preceding commit,
                when that commit reported pending pages

estimated DB growth(i,c) = positive DB delta(c) × changed(i,c) / total changed(c)
estimated free reuse(i,c) = positive free decrease(c) × changed(i,c) / total changed(c)
estimated free growth(i,c) = positive free increase(c) × released(i,c) / total released(c)
```

The one-commit release association follows the pinned runtime: bbolt publishes cached freelist counts when closing a write transaction, then releases eligible pending pages when beginning the next one. Compaction's newly pending pages therefore appear as free in a later commit observation. This ordering is visible in [`Tx.close`](https://github.com/etcd-io/bbolt/blob/v1.4.0/tx.go) and [`beginRWTx`](https://github.com/etcd-io/bbolt/blob/v1.4.0/db.go). The analyzer retains the immediately preceding commit's compaction weights on disk, then rotates them on every commit, including commits with no free increase. A compaction cannot receive credit for unrelated changes arbitrarily far into the future. This rule relies on the offline runtime's synchronous reader release; it is not a reconstruction of arbitrary production reader lifetimes. Deferred contribution bytes are identified separately in the metrics.

Integer allocation uses deterministic cumulative rounding, preserving the observed byte total. When the relevant denominator is zero, the physical change stays unattributed. Copy-on-write page changes can increase free space without preceding compaction; these increases remain unattributed. Current compaction weights are not used as a fallback for a free increase from an unknown prior cause. DB shrink is recorded globally without assigning object ownership.

These are shared-commit estimates, not measurements of exclusive page ownership or counterfactual causal effects. Mixed-object pages, freelist work, and non-MVCC metadata can contribute to the same commit. Compaction-weighted attribution can associate those shared costs with active objects. The report retains exact per-window record accounting, global physical measurements, estimated shares, source index ranges, and the unattributed remainder. Per-commit weights are consumed during analysis; reproducing them requires replaying the preserved capture with the documented rule.

Free growth and reuse are gross sums across commits in the window. Their difference equals the window's net free-space change after adding the unattributed difference. Gross free growth can be substantial even when later writes reuse most of it. `free-net` ranks positive per-contributor estimated net changes; negative contributions remain available in row metrics as reuse exceeding growth.

## Time and sparse output

Normal windows are half-open `[start,end)`. The final window includes finalization at the last replay timestamp. An operation exactly at a window boundary belongs to the new window; a final boundary-only window can therefore have zero duration and still contain activity.

Logical mutations are assigned to the window in which they execute. Physical effects are assigned to the window in which their backend commit occurs. Pending contribution weights carry across window boundaries; an object can receive a physical estimate in a later window without a new write there. No additional replay commit is forced to close an analysis window.

Object/resource rows are sparse: they are emitted only when logical stock, workload counters, or an estimated physical contribution changes. Each row includes start/end stock. Unchanged objects carry forward from the baseline or their previous row. Global window rows always include complete aggregate stock and physical endpoints. Positive net-growth ranking percentages use the sum of all positive contributors, not the truncated top list or a global net that could include other objects shrinking.

## Output contract and scale

The replay's `report.json` contains the analysis summary and names the `analysis/` directory:

| Artifact | Contents |
| --- | --- |
| `summary.json` | Model, window configuration, totals, file references, and JSONL SHA256 hashes. |
| `baseline.objects.jsonl`, `baseline.resources.jsonl` | Initial stock, including historical versions. |
| `objects.jsonl`, `resources.jsonl` | Complete sparse per-window rows, independent of top-N truncation. |
| `windows.jsonl` | Global window endpoints, activity, attribution totals, and object/resource rankings. |
| `final.objects.jsonl`, `final.resources.jsonl` | Final retained inventory. |
| `analysis.db` | Derived disk-backed object state and aggregation work area. |

The analysis DB stores numeric per-key accounting, selected Kubernetes metadata, and aggregates, not raw values or all revision payloads. Dirty work is checkpointed in bounded batches and at commits/windows. The side DB uses `NoSync` while deriving results, then syncs at successful completion; it is rebuildable and is not a crash-resumable recording journal. JSONL is streamed, and top lists retain at most the configured count per metric/scope.

Storage grows with distinct keys observed and emitted changed-window rows. Deleted key metadata can remain in the derived DB. The replay's MVCC index, bbolt working set, and baseline validation still have their own memory requirements. These choices remove retention of the week's payloads and full observation history in Go result slices; they do not establish a constant process RSS for every possible keyspace.

## Kubernetes identity

The full etcd storage key is the object identity. Default `/registry` layouts yield a storage prefix, namespace when present, and name. Known service prefixes such as `services/specs` remain distinct. DNS-like group/resource layouts can provide a group hint, with explicit path-layout confidence. A key-derived resource is a storage prefix, not a discovery-verified GVR or Kind. For example, legacy prefixes can be `minions` or `controllers`.

Nonstandard prefixes and malformed paths remain unclassified; raw keys are preserved. Non-UTF-8 keys use base64 with an encoding discriminator. Delete/recreate at the same path is one storage-object series; Kubernetes UID incarnations are not separated. Encryption does not prevent key/length attribution, but serialized/encrypted bytes are not decoded Kubernetes object sizes.

Path rules were checked against pinned Kubernetes source:

- [Default resource-prefix overrides](https://github.com/kubernetes/kubernetes/blob/v1.34.0/pkg/kubeapiserver/default_storage_factory_builder.go)
- [Default storage factory](https://github.com/kubernetes/kubernetes/blob/v1.34.0/staging/src/k8s.io/apiserver/pkg/server/storage/storage_factory.go)
- [Simple storage factory group/resource prefixes](https://github.com/kubernetes/kubernetes/blob/v1.34.0/staging/src/k8s.io/apiserver/pkg/server/options/etcd.go)
- [Generic storage key construction](https://github.com/kubernetes/kubernetes/blob/v1.34.0/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go)

## Configurable Kubernetes metadata and tenant aggregation

`object-analysis-v3` includes selected metadata in object rows and baseline/final inventories. The viewers also accept older v2 outputs, reporting metadata as not recorded. Metadata extraction happens entirely during offline replay; the recorder needs no Kubernetes API credentials, discovery, List, or Watch.

```sh
bin/analyzer --capture captures/session --out runs/tenants \
  --speed max --window 5s --top 20 \
  --label-key example.com/tenant --label-key example.com/team \
  --label-key app.kubernetes.io/name,app.kubernetes.io/part-of
```

The keys above are examples. `--label-key` can be repeated or comma-separated, accepts up to 64 distinct keys, and applies to both workload and Namespace objects. The default extracts no labels. Adding keys later requires replaying the existing capture into a new output directory; the original source does not need to be contacted.

The extractor recognizes Kubernetes JSON and the `k8s\x00` protobuf envelope and keeps API version/group, kind, namespace/name, UID, allowlisted labels, the immediate controller owner reference, and the sorted unique managedFields manager set. It skips spec, status, data, annotations, and managed-field ownership trees. Tests include an independently encoded upstream Kubernetes protobuf fixture; see `internal/analysis/testdata/README.md`. Opaque, encrypted, malformed, unavailable, and identity-mismatched metadata remain explicit classifications. Encryption does not prevent storage-key accounting, but encrypted labels are unavailable.

The query layer supports these additional dimensions:

| Dimension | Attribution |
| --- | --- |
| Namespace | Decoded namespace, with storage-path fallback; cluster and unknown keys are explicit groups. |
| API group / kind | Decoded group and kind; group hints from storage paths remain labeled as hints. |
| Object label | One configured key's value; missing, empty, and unavailable are distinct. |
| Namespace label | Join the object's namespace to the recorded Namespace object at that window's end. |
| Controller owner | Immediate owner, partitioned by the dependent object's namespace, API version, kind, name, and UID. |
| Manager set | Entire sorted unique managedFields manager set, counting each object once. This is not an authenticated last-writer identity. |

For namespace-label grouping, a Namespace object's own bytes remain cluster-scoped; its labels classify its namespaced child objects. Missing Namespace records are not guessed. OwnerReference does not include an owner namespace: the grouping's namespace is explicitly the dependent object's namespace, and owner scope is not discovered. Owner grouping does not traverse a ReplicaSet to its Deployment; the immediate owner and its UID are shown so that the owner can be inspected separately. UID changes are exposed in object metadata while the object series continues to follow the storage key.

Classification uses the latest observed metadata **at each analysis-window endpoint**. All activity within that object/window uses that classification; changes inside a window are not split. Namespace joins use historical endpoint state, never the final-run Namespace inventory. Deletes and compaction preserve the object's last observed metadata; historical record removal and physical estimates use that then-current classification rather than labels from every removed revision. Smaller `--window` values reduce this temporal ambiguity.

Group counters sum all matching object/window rows. History and retained growth sum each row's signed stock delta across the interval, then rank positive group totals. They do not subtract unrelated objects' first/last stocks, double-count multiple managers, or reinterpret a label change as a physical transfer of stored pages. Missing metadata remains in the denominator; global physical changes and unattributed bytes stay visible independently of filters.

Optional calibration, source-native physical models, discovery-verified GVRs, owner-chain resolution, separate UID incarnation series, and counterfactual replay remain extensions.
