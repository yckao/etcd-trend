# etcd trend

> [!WARNING]
> This project is currently **pure vibe coding and has not been manually reviewed by a human**. Automated tests and CI results do not substitute for human review.

[![Test and publish image](https://github.com/yckao/etcd-trend/actions/workflows/ci.yml/badge.svg)](https://github.com/yckao/etcd-trend/actions/workflows/ci.yml)

Record an etcd member's WAL and replay its committed operations offline through etcd's MVCC and bbolt backend. The analyzer supports accelerated virtual time so the same capture can be inspected without waiting for its original duration.

The recorder reads WAL files and accepts a consistent baseline from an explicit snapshot provider. It does not open an etcd client connection, watch the keyspace, manage the source process, or alter source retention settings. Filesystem reads still consume resources; production collection overhead must be qualified for the chosen deployment.

The recorder and analyzer accept stable etcd **3.4.x, 3.5.x, and 3.6.x** sources. Offline storage simulation uses a pinned **3.6.0** engine with explicit adapters for older internal metadata. Source compatibility means interpreting and validating captured logical history; physical space curves are produced by the selected 3.6.0 model. The source member does not need patches. Calibration is optional.

See [the design](docs/design.md) for the capture contract, virtual scheduler, and implementation boundaries.

## Install a single binary

```sh
go install github.com/yckao/etcd-trend@latest
etcd-trend help
```

The installed `etcd-trend` executable contains `record`, `replay`, `web`, `tui`, `inspect`, `demo`, `loadtest`, and `compat`. The Web assets and modified replay engine are compiled into it; companion executables and a local source checkout are not required. Replace `@latest` with a commit or release version to pin an installation. The `loadtest` command invokes this same executable for its recorder and analyzer child processes.

The source-build examples below use `bin/etcd-trend`; with `go install`, use `etcd-trend` from your Go binary directory instead. Previous standalone executable names have been replaced by subcommands, for example `analyzer ...` becomes `etcd-trend replay ...`.

## Build and test

Go 1.24 or later is required.

```sh
make build
make test
make vet
```

`make build` produces only `bin/etcd-trend`; `make install` installs the current checkout through Go.

The integration test starts its own temporary etcd member on loopback ports. It does not access a kubeconfig or an existing cluster.

## Record

Create the capture parent directory, then select an immutable baseline and the member's WAL directory:

```sh
mkdir -p captures
bin/etcd-trend record \
  --wal-dir /var/lib/etcd/member/wal \
  --snapshot /consistent-snapshots/baseline.db \
  --etcd-version 3.6.0 \
  --out captures/session \
  --duration 1h
```

Omit `--duration` to record until SIGINT or SIGTERM. Graceful stop seals and validates the captured prefix. Incomplete or failed sessions are rejected by the analyzer. This milestone starts new sessions only; it does not resume a crashed recorder.

`--etcd-version` is required because the recorder does not query the member to discover its release. Supply the actual source patch version; it is preserved in the manifest and replay report.

For a deployment-specific snapshot provider, replace `--snapshot` with an explicit command array:

```sh
--snapshot-command '["/usr/local/bin/etcd-snapshot-provider"]'
```

The executable must write the consistent image to `CAPTURE_SNAPSHOT_PATH` and exit successfully. It is invoked directly, without an implicit shell. The recorder checks the resulting copy before marking the baseline ready.

## Replay

```sh
mkdir -p runs
bin/etcd-trend replay --capture captures/session --out runs/session-100x --speed 100
bin/etcd-trend replay --capture captures/session --out runs/session-max --speed max
```

Each run creates `replay.db`, a compact `report.json`, and streamed `operations.jsonl` / `commits.jsonl`. The report includes source and simulation-engine versions, the final revision and logical hash, observation counts, and observation-file hashes. Commit records contain DB size, in-use size, free pages, and pending pages. Input captures remain unchanged. Outputs must use a new directory.

Object analysis is enabled by default. Add `--window 1m --top 20` to select virtual-time windows and ranking depth. `analysis/` contains exact per-object/resource MVCC accounting and estimated shares of physical DB/free-space changes, with an explicit unattributed remainder. Inspect a window directly:

```sh
bin/etcd-trend inspect --run runs/session-max --window 0 --metric history
bin/etcd-trend inspect --run runs/session-max --window 0 --metric free-net
```

See [object analysis](docs/object-analysis.md) for metric definitions, allocation formulas, sparse output, and all ranking options. Use `--object-analysis=false` to omit this analysis.

For interactive offline exploration, open the same completed run in either interface:

```sh
bin/etcd-trend web --run runs/session-max
bin/etcd-trend tui --run runs/session-max
```

Both provide DB/free-space curves, interval-based resource/object rankings, search, and individual object timelines. The Web UI uses an embedded local server with no external assets. See [offline UI usage](docs/offline-ui.md) for controls, window resolution, and index behavior.

The TUI supports 60% keyboards without a navigation-key layer: `c` opens the command menu, `r` edits a range such as `2m 5m`, `j/k` selects rows, `d/u` changes pages, `t/b` selects first/last, and `e` goes back. Use Enter to drill down and `q` to quit. In text prompts, Ctrl-U clears the input and Ctrl-G cancels. Grouping, metrics, label keys, and filters are selectable from the command menu; the previous bindings remain available.

For multi-tenant attribution, extract your chosen label keys while replaying:

```sh
bin/etcd-trend replay --capture captures/session --out runs/tenants --window 5s \
  --label-key example.com/tenant --label-key example.com/team
bin/etcd-trend web --run runs/tenants
bin/etcd-trend tui --run runs/tenants --scope namespace-label \
  --label-key example.com/tenant
```

Label keys are configurable; none are extracted by default. Both viewers group by namespace, API group/kind, object label, historical Namespace label, immediate controller owner, and managedFields manager set. Metadata comes from captured JSON/protobuf values during offline replay. Encrypted or unavailable metadata stays explicit. See [attribution semantics](docs/object-analysis.md#configurable-kubernetes-metadata-and-tenant-aggregation) for window-end classification, controller scope, and manager limitations.

Preparation validates the complete capture and builds a disk index of committed entries, resolving overwritten Raft suffixes without retaining WAL payloads in memory. Replay then reads payloads from their original files. The temporary index is removed when the command exits; use `--index-dir NEW_DIR` to retain it for inspection. Each invocation currently rebuilds its index. Allow space for the capture, replay DB, observation logs, and indexes.

Use `--batch-interval`, `--batch-limit`, `--compaction-batch-limit`, and `--compaction-sleep` to select the simulation parameters. Defaults match the reference version's common backend settings; they are not automatically discovered from the source. Keep them fixed when comparing playback speeds.

`--speed 100` requests 100x pacing, not guaranteed 100x throughput. Actual execution is bounded by decode, apply, and storage work. `--speed max` removes pacing waits while preserving the same virtual schedule.

## Single container image

The public image is [`ghcr.io/yckao/etcd-trend:latest`](https://github.com/yckao/etcd-trend/pkgs/container/etcd-trend), with Linux amd64 and arm64 variants under one image reference. Use a published digest for a fixed build. The image contains the same single compiled `etcd-trend` executable as `go install`, and uses it directly as its entrypoint. The same image runs each stage:

| Image command | Purpose |
| --- | --- |
| `record` | Preserve a member's WAL with an explicit consistent baseline |
| `replay` | Accelerated offline replay and metadata extraction |
| `web` | Loopback-only offline Web UI |
| `tui` | Interactive terminal explorer; allocate stdin and a TTY |
| `inspect` | Print a selected analysis window |
| `demo`, `loadtest`, `compat` | Isolated validation tools |

The default identity is UID/GID `65532:65532`. Host-WAL examples below explicitly use root to read existing root-owned files; use an appropriately authorized UID instead when available. Mount source WAL and baseline read-only, and provide dedicated writable capture/output directories. The full [runtime guide](docs/container-runtimes.md) covers preparation, ownership, SELinux, graceful stops, replay, Web, and TUI.

### containerd

Use `nerdctl` on the Linux node. After preparing the paths and immutable baseline described in the [guide](docs/container-runtimes.md#prepare-the-host-paths):

```sh
IMAGE=ghcr.io/yckao/etcd-trend:latest
sudo nerdctl --namespace default pull "$IMAGE"
sudo nerdctl --namespace default run --name etcd-trend-recorder -d \
  --user 0:0 --read-only --network none --stop-timeout 3600 \
  --mount type=bind,src=/var/lib/etcd/member/wal,dst=/source/wal,ro \
  --mount type=bind,src=/srv/etcd-trend/baselines/baseline.db,dst=/source/baseline.db,ro \
  --mount type=bind,src=/srv/etcd-trend/captures,dst=/captures \
  --mount type=bind,src=/srv/etcd-trend/tmp-recorder,dst=/tmp \
  "$IMAGE" record --wal-dir /source/wal --snapshot /source/baseline.db \
  --etcd-version 3.6.0 --out /captures/session-001 --duration 1h
```

Set the actual source patch version and host paths. After recording, run `replay` with the completed capture mounted read-only, then `tui` with `-it` or `web` with `--network host --listen 127.0.0.1:8080`. Web accepts loopback only; use SSH forwarding for remote access. See the [complete containerd commands](docs/container-runtimes.md#containerd-with-nerdctl).

### CRI-O

Use `crictl` against CRI-O's socket. Edit the checked-in [sandbox](deploy/crio/sandbox.json), [recorder](deploy/crio/recorder.json), and [offline workbench](deploy/crio/workbench.json) configs with the node's paths, source version, session name, and a fresh sandbox UID:

```sh
cri() {
  sudo crictl --runtime-endpoint unix:///run/crio/crio.sock \
    --image-endpoint unix:///run/crio/crio.sock "$@"
}
cri pull ghcr.io/yckao/etcd-trend:latest
POD_ID=$(cri runp deploy/crio/sandbox.json)
RECORDER_ID=$(cri create "$POD_ID" deploy/crio/recorder.json deploy/crio/sandbox.json)
cri start "$RECORDER_ID"
cri logs -f "$RECORDER_ID"
```

The recorder exits after its configured duration. Confirm a successful exit and a `complete` capture manifest before replay. The workbench runs offline commands through `cri exec`; use `cri exec -it` for the TUI. Its host-network sandbox allows the Web listener to remain on loopback. See the [complete CRI-O lifecycle](docs/container-runtimes.md#cri-o-with-crictl), including early stop and cleanup. These runtime recipes are source-checked; live CRI-O/containerd node qualification is separate from the image's CI smoke tests.

### GitHub Actions and image tags

[The workflow](.github/workflows/ci.yml) runs race tests, vet, builds, Web interaction checks, and container smoke tests on pull requests. Published commits also verify `go install` outside the source checkout. Both amd64 and arm64 candidates must pass recording, replay, metadata, TUI snapshot, and graceful-stop checks before publication; arm64 executes under QEMU. Pushes to `main` publish `latest` and a full-commit `sha-...` tag. Version tags such as `v0.1.0` additionally publish `0.1.0` and `0.1`. The published multi-platform image includes provenance and an SBOM. GitHub's repository token supplies GHCR access; no manually configured publishing secret is needed.

## Run the isolated demonstration

```sh
mkdir -p artifacts
bin/etcd-trend demo --out artifacts/demo
```

The demo records 240 updates of a 300 KiB value, crosses the native 64 MB WAL segment boundary, and includes successful and failed Txn comparisons, range deletes, retained and revoked leases, and compaction. It compares 1x, 100x, and max replay against the source's complete logical KV state, including revision and lease metadata. It also verifies equal modeled commit boundaries and unchanged capture files. The output directory includes `summary.json`, the raw capture, and individual replay reports.

The test suite uses smaller fixture-only WAL segments for inexpensive rotation coverage. It also tests torn tails, overwritten uncommitted entries, corruption and gaps, and input guards. See [qualification results](docs/qualification.md) for the recorded local run.

## Compatibility and rate qualification

Run an actual release binary in the isolated compatibility fixture:

```sh
bin/etcd-trend compat --etcd-binary /path/to/etcd --out artifacts/compat-run
```

The fixture records source version and binary SHA256, generates Txn, lease, delete, and compaction history, then compares 1x, 100x, and max replay against the source's logical state. Serving-cluster upgrade procedures are independent of this offline reader.

For a separate-process recorder/analyzer measurement at the production planning rate:

```sh
bin/etcd-trend loadtest --out artifacts/load-run --duration 60s \
  --bytes-per-second 2000000
```

This starts only a private loopback member. The rate flag targets value bytes; the summary reports actual captured WAL bytes, CPU time, peak process RSS, index sizes, and replay time. A single member producing 2 MB/s of WAL generates approximately **172.8 GB/day or 1.21 TB/week**, before baselines and analysis outputs. This is a capacity budget, not a week-long qualification result.

For a longer synthetic Kubernetes workload with five real-time tenant phases:

```sh
bin/etcd-trend loadtest --out artifacts/tenants --workload kubernetes \
  --duration 10m --bytes-per-second 2000000 --compaction-interval 45s --record-only
bin/etcd-trend replay --capture artifacts/tenants/capture --out artifacts/tenants/replay \
  --window 5s --speed max --label-key tenant,team,app.kubernetes.io/name \
  --compaction-batch-limit 16 --compaction-sleep 1ms
bin/etcd-trend web --run artifacts/tenants/replay
```

The fixture writes Kubernetes-shaped synthetic values directly to its private etcd, with four namespaces, owner references, manager sets, quiet objects, changing tenant labels, and delete/recreate churn. It does not run Kubernetes controllers. `--record-only` preserves the capture and source digest for separate analyzer runs; zero replay fields in that recording summary mean replay has not run yet. The completed longer run in this workspace is `artifacts/long-multitenant-10m-validated/replay-tenants`.

## Snapshot providers

The recorder accepts either an existing immutable etcd backend snapshot or an explicit provider executable that writes a consistent snapshot to `CAPTURE_SNAPSHOT_PATH`. The provider runs while WAL acquisition is active. There is no automatic filesystem-specific provider in this milestone, and no implicit online Snapshot RPC.

An ordinary copy of the live mutable bbolt file is not a snapshot provider. Use a consistent storage snapshot or a separately obtained immutable baseline. The demonstration creates its baseline while its own synthetic member is stopped; that lifecycle is restricted to the fixture.

## Implementation boundaries

- Replay uses the actual etcd apply, MVCC, lease, and bbolt code paths.
- Backend timer commits use virtual time. Native operation-triggered commits remain active.
- Physical compaction runs serially in this milestone. Its batch waits advance virtual time; future WAL writes do not interleave with that compaction. This is a reproducible model, not a reconstruction of original background scheduling.
- The recorder's timestamps describe when WAL prefixes were observed. They are not server-side commit timestamps.
- Defrag is not represented by Raft WAL entries. Its timeline and optional calibration inputs are design extension points.
- Object/resource window analysis accounts for actual mutations and history, and estimates physical effects from shared commits. Exclusive page ownership and counterfactual causal effects are not measured.
- WAL payloads, time anchors, and result observations use streaming or disk indexes. Memory still depends on the recovered etcd/MVCC working set, baseline validation, the largest request, and segment metadata. Full week-long inputs remain unqualified.
- Stable 3.4–3.6 source versions are accepted; the tested release matrix and operation coverage are listed in the qualification document. True etcd v2 user-keyspace requests remain unsupported.
- Raft membership-change entries fail explicitly in this runtime. Normal member-attribute updates on restart are supported.
- Captures contain raw etcd values. Files are created with private permissions and should be stored accordingly.

## Upstream code

`third_party/etcd-server` contains the modified etcd v3.6.0 server source with its upstream license. It is part of this project's Go module, so remote installation needs no local `replace` directive. [Local replay changes](third_party/etcd-server/REPLAY_PATCHES.md) are documented alongside it. Other dependencies are pinned in `go.mod` and `go.sum`.
