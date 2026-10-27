# Running the image with containerd and CRI-O

`ghcr.io/yckao/etcd-trend:latest` contains the recorder, analyzer, Web UI, TUI, inspector, and isolated fixture tools in one image. Select a command after the image name: `record`, `replay`, `web`, `tui`, or `inspect`. The image supports Linux amd64 and arm64. For repeatable investigations, replace `latest` with the published image digest.

These examples run directly on a Linux host; they do not create Kubernetes API objects. The recorder needs only a read-only WAL mount, a consistent immutable baseline, and a writable capture destination. The offline commands need only completed captures or replay outputs. Do not mount the container runtime socket into the application container.

## Prepare the host paths

The examples use these paths; change them to match the node:

| Host path | Purpose |
| --- | --- |
| `/var/lib/etcd/member/wal` | The selected member's WAL directory, mounted read-only |
| `/srv/etcd-trend/baselines/baseline.db` | An existing consistent, immutable baseline |
| `/srv/etcd-trend/captures` | Captured sessions |
| `/srv/etcd-trend/runs` | New offline replay outputs |
| `/srv/etcd-trend/tmp-recorder`, `tmp-offline` | Writable scratch space |
| `/srv/etcd-trend/logs` | CRI container logs |

```sh
sudo install -d -m 0700 /srv/etcd-trend/baselines \
  /srv/etcd-trend/captures /srv/etcd-trend/runs \
  /srv/etcd-trend/tmp-recorder /srv/etcd-trend/tmp-offline \
  /srv/etcd-trend/logs
```

Supply `baseline.db` using your existing consistent snapshot method, with the WAL needed to bridge its applied index still available. A normal copy of a live mutable `member/snap/db` is not a consistent baseline. The recorder also accepts `--snapshot-command` for an explicitly mounted snapshot provider; see [the snapshot contract](../README.md#snapshot-providers).

The image defaults to UID/GID `65532:65532`. These host examples explicitly use UID 0 so a root-owned WAL and the recorder's private output files remain readable across stages. You can instead use an identity that already has the required read/write access and adjust only the dedicated output directories. Source ownership and permissions need not change.

On SELinux hosts, use a host policy/process label that permits the chosen container to read the existing source labels and write the dedicated destination. The supplied CRI mounts set `selinux_relabel: false`; do not add `:z`/`:Z` to the live etcd WAL mount. UID 0 does not bypass SELinux. The examples do not enable privileged mode or disable host enforcement. The CRI exposes process-label and mount-relabel controls separately. [CRI API definitions](https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto)

## containerd with nerdctl

Use a rootful containerd endpoint that can mount the host's WAL. These examples select containerd's `default` namespace explicitly, keeping the investigation's containers separate from kubelet's usual `k8s.io` namespace. The namespace is a containerd object namespace, not a Kubernetes namespace. `nerdctl` accepts Docker-style image arguments, bind mounts, interactive terminals, and host networking. [nerdctl command reference](https://github.com/containerd/nerdctl/blob/main/docs/command-reference.md)

```sh
IMAGE=ghcr.io/yckao/etcd-trend:latest
sudo nerdctl --namespace default pull "$IMAGE"
```

Record a new session. Set the actual member patch version, WAL path, baseline, and duration:

```sh
sudo nerdctl --namespace default run --name etcd-trend-recorder -d \
  --user 0:0 --read-only --network none --stop-timeout 3600 \
  --mount type=bind,src=/var/lib/etcd/member/wal,dst=/source/wal,ro \
  --mount type=bind,src=/srv/etcd-trend/baselines/baseline.db,dst=/source/baseline.db,ro \
  --mount type=bind,src=/srv/etcd-trend/captures,dst=/captures \
  --mount type=bind,src=/srv/etcd-trend/tmp-recorder,dst=/tmp \
  "$IMAGE" record --wal-dir /source/wal --snapshot /source/baseline.db \
  --etcd-version 3.6.0 --out /captures/session-001 --duration 1h

sudo nerdctl --namespace default logs -f etcd-trend-recorder
```

The recorder exits after its duration and final validation. To end it earlier, use `sudo nerdctl --namespace default stop --time 3600 etcd-trend-recorder`. The grace period must cover the final scan/hash work for your capture size; inspect the exit status and confirm `manifest.json` has `"state": "complete"` before replay. Remove the stopped container with `sudo nerdctl --namespace default rm etcd-trend-recorder` when its logs are no longer needed. Keep the captured files.

Replay, with your own label keys:

```sh
sudo nerdctl --namespace default run --rm --user 0:0 --read-only --network none \
  --mount type=bind,src=/srv/etcd-trend/captures,dst=/captures,ro \
  --mount type=bind,src=/srv/etcd-trend/runs,dst=/runs \
  --mount type=bind,src=/srv/etcd-trend/tmp-offline,dst=/tmp \
  "$IMAGE" replay --capture /captures/session-001 --out /runs/session-001 \
  --speed max --window 5s \
  --label-key example.com/tenant --label-key example.com/team
```

Open the TUI in a real terminal:

```sh
sudo nerdctl --namespace default run --rm -it --user 0:0 --read-only --network none \
  -e TERM="${TERM:-xterm-256color}" \
  --mount type=bind,src=/srv/etcd-trend/runs,dst=/runs,ro \
  --mount type=bind,src=/srv/etcd-trend/tmp-offline,dst=/tmp \
  "$IMAGE" tui --run /runs/session-001
```

The Web server accepts loopback addresses only. Use host networking so its loopback listener is reachable on the host; a bridge-network `-p` mapping cannot reach a process bound only to the container's loopback address:

```sh
sudo nerdctl --namespace default run --rm --user 0:0 --read-only --network host \
  --mount type=bind,src=/srv/etcd-trend/runs,dst=/runs,ro \
  --mount type=bind,src=/srv/etcd-trend/tmp-offline,dst=/tmp \
  "$IMAGE" web --run /runs/session-001 --listen 127.0.0.1:8080
```

Browse `http://127.0.0.1:8080` on that host. From a workstation, use `ssh -L 8080:127.0.0.1:8080 your-node`, then open the same local address. The viewer does not need a live etcd mount.

## CRI-O with crictl

Use `crictl` against the explicit CRI-O socket. The examples are native CRI sandbox/container configs, following CRI-O's `runp → create → start` lifecycle. A Podman invocation would use a separate engine. [CRI-O crictl tutorial](https://github.com/cri-o/cri-o/blob/main/tutorials/crictl.md), [crictl usage](https://github.com/kubernetes-sigs/cri-tools/blob/master/docs/crictl.md)

Copy and edit [sandbox.json](../deploy/crio/sandbox.json), [recorder.json](../deploy/crio/recorder.json), and [workbench.json](../deploy/crio/workbench.json). Set actual host paths, image digest, source version, output session name, and duration. Give each new sandbox a fresh metadata UID. `etcd-trend.slice` assumes CRI-O uses the systemd cgroup driver; set `linux.cgroup_parent` to a path appropriate for your configured driver when it differs.

The sandbox uses the host network namespace (`network: 2`) so the Web server can bind host loopback without a CNI attachment or host port mapping. PID isolation remains per container. The workbench mounts captured files, output, and scratch; it never mounts the live WAL.

```sh
cri() {
  sudo crictl --runtime-endpoint unix:///run/crio/crio.sock \
    --image-endpoint unix:///run/crio/crio.sock "$@"
}

cri version
cri pull ghcr.io/yckao/etcd-trend:latest
POD_ID=$(cri runp deploy/crio/sandbox.json)
RECORDER_ID=$(cri create "$POD_ID" deploy/crio/recorder.json deploy/crio/sandbox.json)
cri start "$RECORDER_ID"
cri logs -f "$RECORDER_ID"
```

The configured duration ends recording gracefully. For an early stop, request a long enough grace period and inspect the result before removing anything:

```sh
cri stop --timeout 3600 "$RECORDER_ID"
cri inspect "$RECORDER_ID"
sudo cat /srv/etcd-trend/captures/session-001/manifest.json
```

Continue only after the recorder exited successfully and the manifest state is `complete`. A forced kill can leave an incomplete capture. Stop timeout is the grace period before the runtime may force termination. [crictl stop implementation](https://github.com/kubernetes-sigs/cri-tools/blob/master/cmd/crictl/container.go)

Start the offline workbench, then execute the analyzer:

```sh
WORKBENCH_ID=$(cri create "$POD_ID" deploy/crio/workbench.json deploy/crio/sandbox.json)
cri start "$WORKBENCH_ID"
cri exec "$WORKBENCH_ID" /usr/local/bin/etcd-trend replay \
  --capture /captures/session-001 --out /runs/session-001 --speed max --window 5s \
  --label-key example.com/tenant --label-key example.com/team
```

Run the TUI with both stdin and a TTY. `q` exits it while leaving the workbench available:

```sh
cri exec -it "$WORKBENCH_ID" /usr/local/bin/etcd-trend tui --run /runs/session-001
```

Or run the Web UI in the foreground and browse the host's loopback address:

```sh
cri exec "$WORKBENCH_ID" /usr/local/bin/etcd-trend web \
  --run /runs/session-001 --listen 127.0.0.1:8080
```

Disconnecting a CRI exec stream does not serve as a reliable process stop. When finished with all workbench commands, stop that container explicitly. Remove only these recorded IDs; the captured and analyzed files remain in the host directories:

```sh
cri stop --timeout 30 "$WORKBENCH_ID"
cri rm "$WORKBENCH_ID" "$RECORDER_ID"
cri stopp "$POD_ID"
cri rmp "$POD_ID"
```

## Validation boundary

The command arguments match the application CLIs, and the native CRI JSON fields and runtime flags were checked against the official runtime sources linked above. These recipes have not been exercised on a live CRI-O/containerd node in this development session. The image workflow's build/smoke result is separate evidence; it does not qualify a particular node's WAL permissions, SELinux policy, storage snapshot provider, or collection overhead.
