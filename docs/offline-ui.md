# Offline Web UI and terminal UI

Both interfaces read a **completed analyzer output directory** containing `report.json` and `analysis/`. They use the same query layer for curves, interval rankings, object search, and object timelines. They do not connect to etcd or Kubernetes.

## Start

`make build` creates the single `bin/etcd-trend` executable. Alternatively, install it with `go install github.com/yckao/etcd-trend@latest` and use `etcd-trend` in place of `bin/etcd-trend` below; the Go binary installation directory must be on your `PATH`.

```sh
make build

# Produce object analysis if it is not already available.
bin/etcd-trend replay --capture captures/session --out runs/session \
  --window 1m --top 20 --speed max

# Local Web UI: prints the selected loopback URL.
bin/etcd-trend web --run runs/session

# Interactive terminal UI.
bin/etcd-trend tui --run runs/session
```

For the preserved demonstration in this workspace:

```sh
bin/etcd-trend web --run artifacts/object-analysis-native-final
bin/etcd-trend tui --run artifacts/object-analysis-native-final
```

The longer multi-tenant demonstration uses `artifacts/long-multitenant-10m-validated/replay-tenants`. It contains a real ten-minute source workload, five changing workload phases, and metadata extracted with synthetic label keys. Start either viewer with that directory to explore tenant behavior over time.

The Web UI serves embedded HTML, CSS, JavaScript, and charts from the local executable. Runtime operation needs no CDN, external fonts, telemetry, external API, or network connection beyond the local browser-to-server connection. The HTTP listener is restricted to loopback; `--listen 127.0.0.1:8080` selects a fixed local port. Stop the server with Ctrl-C.

The TUI uses terminal raw mode and restores the terminal when it exits. `--snapshot` renders a plain text view without requiring an interactive terminal.

## Curves and interval selection

The overview shows DB size, in-use size, reusable free bytes, and the free-space ratio:

```text
free bytes = DB size - in-use size
free ratio = free bytes / DB size × 100
```

These are observations from the offline storage simulation. Free ratio is the displayed fragmentation proxy; unused space inside allocated B+tree pages is not measured by that ratio. See [the analysis model](object-analysis.md) for physical attribution semantics and pending-page handling.

The chart uses analysis-window boundary observations. Its resolution is the analyzer's original `--window` setting, shown in the interface. Interval selection snaps to those windows. It cannot recover sub-window events from aggregated rows; replay with a smaller window to obtain finer resolution.

Ranges use consecutive window indexes `[from,to)`. Both interfaces display the corresponding elapsed virtual source time. The query layer bounds chart response sizes and retains important extrema when downsampling; point sampling does not change interval accounting or ranking totals.

## Rankings and object drill-down

Select a time interval and a ranking metric:

- **Writes:** actual value bytes written.
- **History:** positive net retained-history growth over the selected interval.
- **Retained:** positive net total MVCC-record growth.
- **Compacted:** record bytes actually removed by physical compaction.
- **DB growth:** estimated association with observed DB growth.
- **Free growth:** estimated association with gross free-space increases.
- **Net free:** positive estimated free growth minus attributed free reuse.

Resource rankings group by the recorded storage prefix. Opening a resource filters the object ranking. Object drill-down shows identity, live/history/retained stocks, mutation and compaction counters, estimated physical contributions, contributing Raft indexes, and a carried-forward timeline. Search also includes quiet baseline objects and deleted objects, including fully compacted ones, even when they have no positive contribution in the current ranking.

Interval rankings are recomputed from **all sparse contributor rows**. They are independent of the analyzer's stored top-N lists. Net growth is calculated after aggregating the entire selected interval, so an object's growth and subsequent shrinkage can cancel. Percentages use the denominator shown by the interface, including the full matching population before pagination; global physical totals and unattributed changes remain visible separately.

Objects without an event in a window keep their preceding state. Their selected-interval counters include every window even when the displayed timeline is sampled. Full key identity and mapping confidence remain available; storage-prefix labels are not discovery-verified Kubernetes GVRs. Values are not loaded or displayed.

## Tenant dimensions and filters

In the Web UI, **Group by** adds namespace, API group, kind, object label, namespace label, immediate controller owner, and managedFields manager set. Select an extracted key under **Configured label key**. The namespace filter and optional exact object-label value filter apply to interval rankings; an explicitly empty label value is supported. Click a group to list its contributing objects, then open an object for metadata and its timeline. The filter chip removes the selected group. Group drill-down clears the group display search while retaining namespace and label filters. Object detail shows the full selected interval for that key, independent of ranking tenant/label filters, so an object that changes tenant may have larger detail totals than its filtered ranking. The independent all-object search remains a search across all observed keys.

Labels are configured when running the analyzer, not fetched by the UI:

```sh
bin/etcd-trend replay --capture captures/session --out runs/tenants \
  --window 5s --speed max \
  --label-key example.com/tenant --label-key example.com/team

bin/etcd-trend tui --run runs/tenants --scope namespace-label \
  --label-key example.com/tenant

bin/etcd-trend tui --run runs/tenants --snapshot --scope object \
  --namespace team-a --label-key example.com/team --label-value platform
```

Use your own keys; the names above are placeholders. Without configured keys, label grouping is disabled. Older v2 outputs still support path-derived namespace/resource analysis, while metadata is reported as not recorded.

Grouping uses the object's latest observed metadata at **each window endpoint**, including historical Namespace joins. Object details show interval-start/end metadata and UID changes. All activity within one window shares that endpoint classification. Compaction uses the current classification, not labels from each removed historical revision. Missing/encrypted metadata remains in explicit groups. Manager sets describe field ownership hints and do not establish the authenticated writer; controller references are immediate owners partitioned by the dependent namespace. See [metadata attribution semantics](object-analysis.md#configurable-kubernetes-metadata-and-tenant-aggregation).

## TUI controls

The interactive view needs at least 72 columns and 20 rows. All actions are reachable on a 60% keyboard without a function layer, arrow keys, or dedicated navigation keys. Press **`c`** for the command menu, move with **`j`/`k`**, and press Enter to choose a grouping, metric, label key, range, or filter directly. Queries run asynchronously with cancellation so input and resizing remain responsive during larger scans.

A typical flow is `c` → **Choose grouping** → **namespace**, then `r` → Ctrl-U → `2m 5m` → Enter. Use `d`/`u` to page through contributors, Enter to drill down, and `e` to return.

| Key | Action |
| --- | --- |
| `c` | Open command menu; choose grouping, metric, label key, filters, or help |
| `j` / `k` | Next/previous row; scroll object details or help |
| `d` / `u` | Next/previous page |
| `t` / `b` | First/last page or detail boundary |
| Enter | Apply a menu choice or open a selected group/object |
| `e` | Return from a menu/detail or clear the current group/resource/search |
| `r` | Edit the selected interval using window bounds or elapsed times |
| `h` / `l` | Shift the whole interval left/right |
| `a` | Select all windows |
| `m` | Cycle ranking metric; the command menu also offers direct selection |
| `g` | Cycle grouping dimension; the command menu also offers direct selection |
| `L` | Cycle recorded label keys; the command menu avoids needing Shift |
| `n` | Edit the namespace filter |
| `v` | Edit the exact object-label value filter; an empty value is valid |
| `x` | Clear namespace, label-value, resource, group, and search filters |
| `i` | Toggle object metadata/detail timeline |
| `/` | Edit object search; Enter applies it |
| `o` | Open the object catalog |
| `?` | Show scrollable help; it is also available in the command menu |
| `q` or Ctrl-C | Exit and restore terminal state |

Text editors show the active field separately. **Ctrl-U** clears it, **Ctrl-W** deletes the previous word, **Ctrl-G** cancels, and Enter applies. Printable shortcut letters remain ordinary text while editing, including `q` and `e`.

The range editor accepts either two integer window boundaries such as `10 20` (start inclusive, end exclusive), or two elapsed durations such as `2m 5m` or `30s 90s`. Time ranges expand to the covering analysis windows and must stay within the recording. Invalid input leaves the current selection unchanged and shows an error. The selected bounds and virtual times remain visible after applying.

Existing bindings remain available: arrows mirror `j/k/h/l`, Page Down/Page Up mirror `d/u`, Home/End mirror `t/b`, and Esc mirrors `e` outside editors (or Ctrl-G within editors). Tab still switches resource/object ranking. `[`/`]` move the start boundary, and `{`/`}` move the end boundary; `r` provides the same selection task without punctuation shortcuts.

For a reproducible plain-text view of the last demonstration window:

```sh
bin/etcd-trend tui --run artifacts/object-analysis-native-final \
  --snapshot --from 3 --to 4 --metric free-net --scope object
```

`--from` is inclusive and `--to` exclusive. Web inputs instead label the last included window as **Through window**. Snapshot mode also accepts `--search`, `--object ID`, `--width`, and `--height`.

`--scope` accepts all grouping dimensions. `--namespace`, `--label-key`, and explicit `--label-value` set filters. `--metadata --object ID` renders object metadata in snapshots. The metadata view includes configured labels, UID, API/kind, controller, manager set, decoding status, and changes from the selected interval's start.

## Indexing and input integrity

On startup, the query layer validates the completed replay report, matching analysis summary, and the seven consumed JSONL files' checksums before exposing results. It builds derived indexes outside the replay directory. It reads the report, summary, and JSONL files; viewing requires no access to the capture payloads or replay backend. The input run remains read-only.

The default cache is a temporary directory removed on exit. `--cache NEW_DIRECTORY` selects a new retained derived-index directory outside the run. The current implementation rebuilds an index on each launch; it does not resume or trust an existing cache directory.

Rows and object metadata use disk-backed indexes. Range aggregation also uses a disk work area rather than a map containing every object in a long recording. Charts and tables return bounded or paginated responses. Startup validation and range queries still perform real I/O proportional to the data involved; long recordings can require substantial indexing time and additional disk capacity. The source recording's payloads are not loaded by these viewers.

The HTTP API is read-only, and browser content uses local assets. Untrusted storage keys are rendered as text; terminal text is sanitized before rendering. An incomplete or damaged analysis is rejected rather than shown as a completed run.

## UI integration checks

Go tests cover the HTTP contract, query layer, and TUI behavior. The JavaScript DOM integration test also exercises grouping, filtering, drill-down, legacy outputs, text-safe metadata rendering, and superseded queries. Its optional URL argument runs against an existing local fixture server. jsdom is a test-only dependency:

```sh
npm install --prefix /tmp/etcd-trend-ui-tests --ignore-scripts jsdom@26.1.0
NODE_PATH=/tmp/etcd-trend-ui-tests/node_modules \
  node scripts/test-web-ui.cjs http://127.0.0.1:50088
```

This verifies DOM/data behavior, not browser layout or screenshot rendering.

For real Chromium checks, install Playwright separately and run:

```sh
npm install --prefix /tmp/etcd-trend-browser-tests --ignore-scripts playwright
/tmp/etcd-trend-browser-tests/node_modules/.bin/playwright install chromium
PLAYWRIGHT_MODULE=/tmp/etcd-trend-browser-tests/node_modules/playwright \
  node scripts/test-web-browser.cjs http://127.0.0.1:50088 artifacts/browser-qa
```

The browser check verifies label-filter cancellation, searched-group drill-down, object metadata, and desktop/mobile rendering. It saves screenshots and a result summary. Both test scripts require a completed Kubernetes fixture with `tenant` labels for their live checks; the viewers themselves do not require that label name.
