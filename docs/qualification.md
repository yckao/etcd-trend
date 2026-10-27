# Recording and replay qualification

The initial implementation was exercised locally on macOS arm64 with Go 1.26.5 and the pinned etcd v3.6.0 runtime on 27 October 2026. Source-version and streaming qualification followed on 28 October. All sources were disposable single-member etcd fixtures on loopback. No existing Kubernetes cluster or production etcd member was accessed.

## Object/resource analysis

Object analysis was qualified on 28 October with actual MVCC mutation hooks, offline baseline-history scanning, disk-backed per-key state, per-window JSONL, and the inspector CLI. These original fixtures use `object-analysis-v2`, with the pinned backend's one-commit delay between compaction and free-page publication explicitly represented. The later `object-analysis-v3` adds Kubernetes metadata while preserving that physical attribution model. The earlier same-commit-only association was rejected because real compaction releases appeared in subsequent commits and were consequently left unattributed.

The native 300 KiB hot-object capture was replayed into `artifacts/object-analysis-native-final/` with one-second windows. It retained the source revision 250, five live keys, 253 entries, 75 observed workload commits, and the source logical hash listed below. Exact accounting found 485 puts, 242 deleted keys, 73,728,660 value bytes written, and 48,854,152 MVCC record bytes physically removed across 160 compaction records.

| Virtual window | Observed DB growth | Hot object estimated DB-growth share | Observed net free change | Hot object estimated net free change |
| --- | ---: | ---: | ---: | ---: |
| 0–1 s | 21,839,872 bytes | 21,835,694 bytes | -32,768 bytes | No positive contribution |
| 1–2 s | 25,837,568 bytes | 25,832,507 bytes | 0 | No positive contribution |
| 2–3 s | 25,526,272 bytes | 25,521,127 bytes | 0 | No positive contribution |
| 3–3.749188 s | 2,179,072 bytes | 2,178,554 bytes | 50,036,736 bytes | 49,496,064 bytes |

The identified key was `/registry/widgets/default/hot`, under storage prefix `widgets`. In the final window, observed gross free growth was 50,724,864 bytes: 50,135,040 were associated with the hot object's preceding compaction batches, and 589,824 remained unattributed. The entire run retained 606,208 unattributed free-growth bytes. These are model estimates derived from completed commits, separate from the exact MVCC byte accounting. The hot object was also the dominant writer and retained-history contributor before compaction.

```sh
bin/etcd-trend-inspect --run artifacts/object-analysis-native-final \
  --window 3 --metric free-net --limit 3
```

The 240,902,112-byte capture from the 1.993 MB/s source fixture was analyzed with ten-second windows into `artifacts/object-analysis-load-final/`. It produced 13 windows, 1,664 object rows, 13 resource rows, and approximately 2.98 MB of analysis artifacts including the derived DB. Replay execution took 21.741 s; total analyzer wall time was 22.122 s, user/system CPU 3.405/2.257 s, and peak RSS 54,460,416 bytes. Replay execution includes mutation/commit analysis callbacks but excludes analysis initialization, final inventory, and final output flushing; total analyzer wall time includes those phases. These are a short local measurement with analysis enabled, not a week-long capacity guarantee.

That run matched the source logical hash/revision and all 2,222 commit boundaries. Exact object accounting recorded 7,323 puts, 239,960,064 value bytes written, 7,194 compacted records, and 236,186,257 compacted MVCC bytes. Final live-object count was 131, with 32,831 bytes of retained history. `process-usage.json` preserves resource measurements and comparison statistics.

Physical layouts are approximate: the final analyzed run's DB size differed from the earlier replay by at most 114,688 bytes, and in-use size by at most 81,920 bytes, despite equal logical state and commit schedule. Separate runs with analysis disabled also showed free/pending-page variation. Equality of the short fixture's curves is an observed test result, not a general byte-identical guarantee. The per-window estimates reconcile to each run's own physical observations.

Captured sources 3.4.45, 3.5.34, and 3.6.0 also passed the actual analyzer CLI with object analysis enabled. Each matched its source hash and five live keys, accounting for 197 puts, 98 deleted keys, and 64 physically compacted records. Evidence is in `artifacts/compatibility/<version>/replay-objects/` and `artifacts/object-analysis-compatibility.json`.

Additional automated checks verify:

- Actual selected Txn branches, range deletes, lease-revocation deletes, and baseline history; unexecuted branches never appear as contributors.
- Final retained-record counts and bytes against the completed replay DB.
- Equal logical results and commit curves with analysis enabled/disabled for the targeted fixture, and identical analysis JSONL hashes at 1x/100x/max.
- Native pending-page compaction release, cross-window delayed credit, stale-credit expiry, and leaving unrelated changes unattributed.
- Conservation across object/resource/window totals, overflow-safe proportional allocation, and different rankings for gross versus net free growth.
- Correct whole-window ranking percentages despite top-N truncation and other objects shrinking.
- Cancellation, completed-report checks, JSONL integrity, and confined inspector paths.

## Offline Web UI and TUI

Both viewers were verified on 28 October using completed replay outputs only. The shared query layer validates seven JSONL files, builds indexes outside the input run, and recomputes interval rankings from complete contributor rows. Tests cover a leading interval contributor absent from every stored top-N, net growth cancellation across compaction, quiet baseline state, fully compacted deleted objects, filtered denominators, concurrent pagination, cancellation, input validation, and bounded chart sampling that retains extrema.

Actual browser checks exercised capacity/fragmentation switching, chart drag selection, numeric window bounds, resource-to-object drill-down, object detail, quiet-object search, and ranking pagination at a 502-pixel viewport. In the native fixture's `[3,4)` interval, both viewers identified `/registry/widgets/default/hot` with 49,496,064 bytes of estimated net free contribution and 48,854,152 bytes physically compacted. The unchanged `/registry/configmaps/default/stable` retained its eight-byte live value with zero interval activity. Disconnecting the local server cleared prior rankings and totals; restarting it restored the selected query successfully. Mobile object-axis clipping found during visual inspection was fixed and rechecked.

The TUI was also exercised through a real PTY: Enter drilled from resource to object, search found the quiet baseline object, `q` and Ctrl-C restored terminal configuration/cursor/alternate screen, and resizing from 120×36 to 50×10 and back produced the expected guidance and redraw. Twelve rapid interval changes finished at `[12,13)` with results matching an independent snapshot; stale responses did not replace the final view. On macOS, the terminal comparison excludes only the transient kernel `PENDIN` status bit and records the original/restored flags.

Snapshot mode loaded native and load outputs plus the existing 3.4.45, 3.5.34, and 3.6.0 source fixtures. For the 13-window load analysis, one local snapshot invocation took 0.133 seconds including startup validation/indexing/query, with peak RSS 27,394,048 bytes. It consumed the small derived analysis of the 240,902,112-byte WAL capture, not the WAL itself. This is a short fixture measurement, not week-long UI qualification.

All 13 files in the native input run, including both database files, retained their pre-viewing SHA256 hashes. `make build`, `go test -race ./...`, and `go vet ./...` passed. Evidence is in `artifacts/offline-ui-qa/` (HTTP results, snapshots, process measurements, input hashes, browser preview) and `artifacts/tui-pty-qa/summary.json` with its PTY logs. See [offline UI usage](offline-ui.md) for controls and the original analysis-window resolution limit.

## Kubernetes metadata and tenant aggregation

`object-analysis-v3` extends the v2 accounting with selected Kubernetes metadata. JSON decoding and the Kubernetes protobuf envelope are tested; the protobuf fixture was generated independently using the official Kubernetes v0.31.0 API and apimachinery serializers. The checked-in fixture includes provenance and a reproducer in `internal/analysis/testdata/`. The production binary does not depend on Kubernetes API packages.

Tests cover configurable label allowlists, baseline metadata, updates, deletion/compaction carry-forward, encrypted/opaque/malformed values, storage-key identity mismatches, and raw callback values being excluded from serialization. The shared viewer layer verifies all nine grouping scopes, historical Namespace label joins, label changes within/across windows, quiet state, missing versus empty label values, controller identity isolation, and manager-set grouping without multiplying contributions. Metadata observation uses each object's window endpoint; it does not reconstruct a separate classification for every removed historical revision.

Web checks cover group search followed by drill-down, disabling/re-enabling a nonempty exact-label filter, explicitly empty values, text-safe rendering, stale-response rejection, and legacy v2 output. Real Chromium checks exercised the updated controls, object metadata, and desktop/502-pixel layouts. The TUI was exercised through a real PTY and has focused grouping/filter/drill-down tests. Object detail explicitly shows full history for the selected interval, independent of the ranking's tenant/group/label filters.

The actual analyzer CLI also replayed the existing 3.4.45, 3.5.34, and 3.6.0 captures with v3 metadata enabled. All three retained revision 106 and their previously qualified logical hashes. These older fixtures contain opaque synthetic values, so this check establishes compatible replay with the new analysis path; JSON/protobuf metadata behavior is qualified separately. Results are in `artifacts/compatibility/<version>/replay-metadata/`.

### Ten-minute multi-tenant capture

The completed run is `artifacts/long-multitenant-10m-validated/`. It uses a private etcd 3.6.0 member and writes Kubernetes-shaped JSON directly, with four namespaces, quiet objects, Namespace labels, owner references, manager sets, opaque/encrypted examples, and Event deletion/recreation. It does not run an API server or Kubernetes controllers. Five workload phases last approximately two minutes each, with physical compaction every 45 seconds. The CRD storage prefix is `/registry/example.test/widgets/<namespace>/<name>`; tests validate every generated workload key against its decoded namespace/name. An earlier fixture with a malformed CRD prefix failed the phase assertion and is excluded from this qualification.

| Measurement | Completed run |
| --- | ---: |
| Actual workload duration | 600.003 s |
| Captured WAL | 1,205,301,752 bytes across 19 segments |
| Measured WAL rate | 2.009 MB/s |
| Value bytes written | 1,200,006,033 bytes |
| Puts / deleted keys | 36,640 / 183 |
| Recorder CPU, user + system | 12.188 s |
| Recorder peak RSS | 25,001,984 bytes |
| Max replay execution | 116.056 s |
| Total analyzer wall time | 117.480 s |
| Analyzer CPU, user + system | 37.763 s |
| Analyzer peak RSS | 219,840,512 bytes |
| Analysis resolution | 5 s; 121 windows including the final partial window |
| Object rows / resource rows | 22,637 / 230 |
| Derived analysis size, including analysis DB | 40,844,746 bytes |
| Final revision / current keys | 36,825 / 1,057 |

Replay matched the source's full logical digest, `d7b87792cff7c859852be2465b7cc44291b6163457fdf1186452381650d4b120`, with zero operation errors. All final object identities matched decoded metadata where available. This is approximately 5.1x acceleration including analyzer startup/finalization on this machine, not a guaranteed production multiplier. Recorder process measurements exclude the source/workload; analyzer memory includes the recovered backend and its analysis working set.

The live viewer API verified the following leaders using interior windows to exclude phase-boundary observation jitter:

| Source phase | Queried replay interval | Namespace leader | Namespace label `tenant` | Write share |
| --- | --- | --- | --- | ---: |
| ConfigMap updates | 10–115 s | tenant-alpha | alpha | 92.49% |
| Pod status updates | 130–235 s | tenant-beta | beta | 92.51% |
| Custom-resource updates | 250–355 s | tenant-gamma | gamma | 92.51% |
| Event churn | 370–475 s | tenant-delta | delta | 92.49% |
| ConfigMap updates after tenant relabel | 490–595 s | tenant-alpha | alpha-migrated | 92.49% |

Across all eight non-object grouping dimensions, write totals, estimated DB growth, estimated free growth, and compacted MVCC bytes conserved the complete report totals. Every group's write drill-down also conserved its parent amount. Quiet-object stocks stayed unchanged with zero interval activity; encrypted metadata remained explicitly unavailable. These checks validate aggregation of the existing physical estimates, not exclusive page ownership. All 13 input-run files, including both DBs, retained their pre-viewing SHA256 hashes.

`go test -race ./...`, `go vet ./...`, and `make build` passed. Final-data Web DOM/API integration, real Chromium desktop/mobile interactions, and TUI snapshots also passed. Evidence includes `summary.json`, `analysis-wall-time.json`, `replay-tenants/report.json`, `tenant-query-qa.json`, `viewer-input-integrity.json`, `tui-*.txt`, and `browser-qa/` beneath the completed run. The scripts `scripts/qualify-tenant-queries.py`, `scripts/test-web-ui.cjs`, and `scripts/test-web-browser.cjs` reproduce the query and UI checks against a local fixture viewer.

## Source compatibility: 3.4–3.6

Each source was an actual etcd release process, separate from the offline engine. The workload contained 96 repeated updates, both Txn comparison branches, range deletes, lease grants/revocation, a retained lease, compaction, and restart metadata. Each capture contained 109 committed entries, compacted through revision 66, and ended at revision 106 with five current keys.

| Source | Fixture provenance | Source logical-state match | 1x / 100x / max commit boundaries and space curves |
| --- | --- | --- | --- |
| 3.4.45 | Native build of official source commit `6e72c68a3199ea1885cf8aacf3d0a4d5e2ba2a4a` | Passed | Equal |
| 3.5.34 | Official darwin-arm64 release, archive checked against upstream SHA256SUMS | Passed | Equal |
| 3.6.0 | Official darwin-arm64 release, archive checked against upstream SHA256SUMS | Passed | Equal |

3.4 lacks a matching official darwin-arm64 binary, so its fixture used an isolated native build with Go 1.26.5 and the fixture-only `ETCD_UNSUPPORTED_ARCH=arm64` override. This is source-format compatibility evidence on this host, not a qualification of that serving platform. The fetch script records toolchain, source commit/archive digest, and binary digest. The normal test suite does not download binaries or start external fixtures unless explicitly configured.

Logical comparison includes values, create/mod revisions, versions, and lease IDs. Hashes are compared within each capture, since independently generated lease IDs differ across source runs. Captures were unchanged after replay, and the fixture's original data directory was moved away before analysis. These small version fixtures use the in-memory qualification API; standalone analyzer checks also exercise their captures through disk indexing and streaming.

Evidence is retained in the ignored `artifacts/compatibility/<version>/` directories, including complete captures, replay reports, source logs, summary, and `provenance.json`. `artifacts/compatibility/locations.json` maps the original paths recorded in those summaries to their preserved workspace locations.

To reproduce with explicitly fetched binaries:

```sh
python3 scripts/fetch-etcd-fixtures.py --output /tmp/etcd-trend-fixtures --build-missing
ETCD_TREND_ETCD_34=/tmp/etcd-trend-fixtures/v3.4.45/etcd \
ETCD_TREND_ETCD_35=/tmp/etcd-trend-fixtures/v3.5.34/etcd \
ETCD_TREND_ETCD_36=/tmp/etcd-trend-fixtures/v3.6.0/etcd \
  go test ./tests/compatibility
```

The accepted range is stable 3.4.x–3.6.x; these three patch releases are the tested matrix. Original-minor physical behavior, every patch release, full authorization/alarm histories, genuine v2 user data, Raft membership changes, and mixed-version upgrade histories are not established by this matrix. Reports explicitly identify the pinned 3.6.0 physical simulation engine.

## Single-member 2 MB/s planning rate

The load fixture used 32 KiB values across 128 rolling keys, with physical compaction every five seconds and a target of 2,000,000 value bytes/s. Actual WAL includes framing and metadata; the table reports the measured WAL rate. Recorder and analyzer were separate child processes, so CPU and peak RSS exclude the source server and workload generator.

| Measurement | 60 s target | 120 s target |
| --- | ---: | ---: |
| Actual workload duration | 60.930 s | 120.852 s |
| Captured WAL | 118,921,392 bytes | 240,902,112 bytes |
| Native WAL segments | 2 | 4 |
| Measured WAL rate | 1.952 MB/s | 1.993 MB/s |
| Recorder CPU, user + system | 1.231 s | 2.447 s |
| Recorder peak RSS | 24.51 MB | 24.64 MB |
| Disk entry index | 174,192 bytes | 352,752 bytes |
| Disk anchor index | 8,688 bytes | 17,536 bytes |
| Index preparation | 0.135 s | 0.229 s |
| Max replay execution | 11.645 s | 20.901 s |
| Replay WAL throughput | 10.21 MB/s | 11.53 MB/s |
| Analyzer total wall time | 12.563 s | 21.674 s |
| Analyzer CPU, user + system | 2.274 s | 4.119 s |
| Analyzer peak RSS | 53.56 MB | 53.10 MB |
| Source/replay final revision | 3617 / 3617 | 7325 / 7325 |

Both runs matched the source's complete logical digest and reported zero operation errors. The second run doubled captured WAL without corresponding RSS growth for this bounded-key workload. RSS is process-wide high-water memory, including file-backed pages; it is not a Go heap bound or a promise for larger MVCC indexes. CPU time divided by wall time put the recorder at approximately 2% of one core in these fixtures. This does not measure production storage latency impact.

Raw summaries are `artifacts/load-2mbps/summary.json` and `artifacts/load-2mbps-120s/summary.json`, with captures, indexes, JSONL observations, and separate process logs. Replay execution excludes preparation and startup; analyzer total wall time includes those phases. Runs used backend batching of 100 ms / 10,000 operations and compaction batches of 16 with 1 ms virtual waits.

The 120-second case replayed about 5.8 times its recorded duration. That is measured throughput for this particular workload and machine, not a guaranteed multiplier. The one-week raw WAL budget at the supplied single-member rate remains 1.2096 TB before snapshots and outputs; a full-week input was not exercised.

## Native WAL demonstration

The fixture wrote 240 updates to a 300 KiB value, together with event keys, successful and failed Txn comparisons, lease grant and revoke, range deletes, and MVCC compaction. The recorder acquired WAL while those writes ran, using an immutable baseline created before the fixture workload.

The capture contained two native WAL segments and 253 committed entries. Compaction targeted revision 162. The source ended at revision 250 with five keys.

The source and all replay runs had the same logical SHA256, including each key's value, create revision, mod revision, version, and lease ID:

```text
57f89e4b385a1e35a0bad7594344ca7554e8bc59da8ed77d0df1383f0e04e89b
```

| Pacing | Replay execution | Virtual duration | Modeled commits | Final DB size | Final in-use size |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1x | 3.761 s | 3.749 s | 75 | 75,481,088 bytes | 25,444,352 bytes |
| 100x | 0.831 s | 3.749 s | 75 | 75,481,088 bytes | 25,444,352 bytes |
| max | 0.835 s | 3.749 s | 75 | 75,481,088 bytes | 25,444,352 bytes |

Commit boundaries and the entire observed space curve were equal across these three runs. Capture checksums were unchanged after replay. Physical equality here compares replay runs under the same model; the source's physical layout was not asserted equal to that model.

Execution timings exclude capture loading, baseline copying, runtime initialization, and report writing. The 100x setting controls pacing; this workload achieved about 4.5x the 1x execution rate because actual apply and storage work dominated. These timings characterize this short local fixture, not a throughput guarantee for production recordings.

The demonstration explicitly used a compaction batch limit of 16 and a 1 ms virtual batch wait to exercise multiple compaction batches. Backend batching used 100 ms and 10,000 operations. Physical compaction followed the serial model described in the design.

Raw results generated in this workspace are in the ignored `artifacts/qualification/summary.json`, its `capture/`, and the three `replay-*/report.json` files. A later standalone analyzer invocation on that same capture also matched revision 250, the logical digest, and 75 commits.

## Automated checks

The following checks passed:

```sh
go test -race ./...
go vet ./...
go test go.etcd.io/etcd/server/v3/storage/backend go.etcd.io/etcd/server/v3/storage/mvcc go.etcd.io/etcd/server/v3/etcdserver/api/membership
make build
```

The end-to-end test used smaller fixture-only WAL segments to exercise rotation across five segments cheaply. It recorded 109 committed entries and compared 1x, 100x, and max against the source at revision 106. Before replay, the source directory was moved away from its original path; only the capture was needed.

Additional checks cover:

- Acquisition while the native WAL writer holds its lock, without modifying source bytes.
- Canonical replay after an uncommitted suffix is overwritten, excluding remaining uncommitted entries.
- Torn active tails, SHA256 and native CRC failures, missing segments, and missing time anchors.
- Rejection of an obvious live backend path and output paths aliased into the source through symlinks.
- Explicit snapshot-provider argv execution and its output contract.
- Successful and failed Txn comparisons, lease revocation, and multi-batch compaction.
- Normal member-attribute updates on restart, final noop progress persistence, and cancellation during virtual compaction waits.
- Equal logical states and modeled commit observations across playback speeds.
- Disk-index suffix replacement across three native WAL rotations, anchor transitions, and committed cutoff.
- A 50.34 MB payload stream with a 36,864-byte entry index; retained heap stays below 12 MiB above its pre-preparation baseline.
- Streaming observation counts and SHA256 hashes, bounded summary output, no retained result slices, and source-version validation.
- Output/index path isolation, including symlink aliases, preserving capture immutability.

The recorder and analyzer binaries were also invoked separately against a stopped synthetic source and its immutable baseline. That smoke check validates CLI wiring; the integration test and native demonstration provide the concurrent recording evidence.

## Remaining qualification

Production snapshot-provider integration, acquisition overhead on the target storage, week-long capture sizes, burst tolerance, recorder crash recovery, and source releases or operations outside the stated matrix remain unqualified. Historical payloads and observations now stream through disk-backed paths; recovered MVCC state and baseline/backend memory remain workload dependent. Calibration, discovery-verified resource identities, owner-chain resolution, separate UID incarnation series, and counterfactual causal experiments remain extensions to the implemented object accounting and physical association model.
