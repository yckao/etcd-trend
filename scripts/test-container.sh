#!/bin/sh
# Integration smoke for a locally loaded image. Uses only a new temporary volume
# and private in-container loopback etcd fixtures; no host etcd/runtime mounts.
set -eu
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf 'Usage: %s IMAGE [PLATFORM]\n' "$0" >&2
    exit 2
fi
image=$1
platform=${2:-}
test_dir=$(mktemp -d)
record_container=
web_container=
cleanup() {
    if [ -n "$record_container" ]; then docker rm -f "$record_container" >/dev/null 2>&1 || true; fi
    if [ -n "$web_container" ]; then docker rm -f "$web_container" >/dev/null 2>&1 || true; fi
    rm -rf "$test_dir"
}
trap cleanup EXIT HUP INT TERM

docker_run() {
    if [ -n "$platform" ]; then
        docker run --platform "$platform" "$@"
    else
        docker run "$@"
    fi
}
run_image() {
    docker_run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev \
        --user "$(id -u):$(id -g)" -v "$test_dir:/data" "$image" "$@"
}

test "$(docker image inspect --format '{{.Config.User}}' "$image")" = 65532:65532
docker_run --rm --network none --read-only --entrypoint /bin/sh "$image" -c '
    test "$(id -u)" = 65532
    test "$(find /usr/local/bin -maxdepth 1 -type f | wc -l)" -eq 1
    test "$(od -An -t x1 -N 4 /usr/local/bin/etcd-trend | tr -d " \n")" = 7f454c46
'
run_image help > "$test_dir/help.txt"
grep -q 'vibe coding' "$test_dir/help.txt"
grep -q 'manually reviewed' "$test_dir/help.txt"
for command in record replay web tui inspect demo loadtest compat; do
    run_image "$command" --help > "$test_dir/$command-help.txt" 2>&1
done

# Exercise native WAL recording and three playback speeds in the image.
run_image demo --out /data/demo --updates 12 --value-bytes 4096 --interval 1ms > "$test_dir/demo.stdout.json"
# Exercise record/replay child processes launched from the same executable.
run_image loadtest --out /data/load --workload kubernetes --duration 3s \
    --bytes-per-second 262144 --value-bytes 4096 --keys 8 --record-only > "$test_dir/load.stdout.json"
run_image replay --capture /data/load/capture --out /data/replay --speed max \
    --window 100ms --label-key tenant --compaction-batch-limit 16 --compaction-sleep 1ms > "$test_dir/replay.stdout.json"
run_image tui --run /data/replay --snapshot --scope namespace-label --label-key tenant > "$test_dir/tui.txt"
run_image inspect --run /data/replay --window 0 --metric writes > "$test_dir/inspect.txt"

# The viewer remains loopback-only even inside the image. Bash's built-in TCP
# support probes that private loopback without adding a runtime HTTP client.
web_container=$(docker_run -d --network none --read-only --tmpfs /tmp:rw,nosuid,nodev \
    --user "$(id -u):$(id -g)" -v "$test_dir:/data:ro" "$image" web \
    --run /data/replay --listen 127.0.0.1:8080)
attempt=0
web_ready=false
while [ "$attempt" -lt 30 ]; do
    if docker exec "$web_container" /bin/bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /api/info HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3; cat <&3' > "$test_dir/web-response.txt" 2>/dev/null; then
        web_ready=true
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "$web_ready" != true ]; then
    docker logs "$web_container" >&2
    printf 'Web viewer did not become ready\n' >&2
    exit 1
fi
docker stop --time 15 "$web_container" > /dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$web_container")" = 0

# Verify the binary running as PID 1 handles SIGTERM and seals a capture.
record_container=$(docker_run -d --network none --read-only --tmpfs /tmp:rw,nosuid,nodev \
    --user "$(id -u):$(id -g)" -v "$test_dir:/data" "$image" record \
    --wal-dir /data/demo/source-stopped/member/member/wal \
    --snapshot /data/demo/source-stopped/baseline.db --out /data/stopped-capture \
    --etcd-version 3.6.0 --poll-interval 20ms)
ready=false
attempt=0
while [ "$attempt" -lt 30 ]; do
    case "$(docker logs "$record_container" 2>&1)" in
        *'recording ready:'*) ready=true; break ;;
    esac
    if [ "$(docker inspect --format '{{.State.Running}}' "$record_container")" != true ]; then break; fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "$ready" != true ]; then
    docker logs "$record_container" >&2
    printf 'Recorder did not become ready\n' >&2
    exit 1
fi
docker stop --time 15 "$record_container" > /dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$record_container")" = 0

python3 - "$test_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
demo = json.loads((root / 'demo/summary.json').read_text())
assert demo['capture_unchanged'] and demo['commit_boundaries_equal']
for run in demo['replays']:
    assert run['logical_sha256'] == demo['source_logical_sha256']
    assert run['revision'] == demo['source_revision']
source = json.loads((root / 'load/summary.json').read_text())
replay = json.loads((root / 'replay/report.json').read_text())
assert source['expected_logical_sha256'] == replay['logical_sha256']
assert source['expected_revision'] == replay['final_revision']
assert replay['operation_error_count'] == 0
assert replay['analysis']['label_keys'] == ['tenant']
assert 'NAMESPACE-LABEL RANKING' in (root / 'tui.txt').read_text()
headers, body = (root / 'web-response.txt').read_bytes().split(b'\r\n\r\n', 1)
assert headers.splitlines()[0] == b'HTTP/1.0 200 OK'
assert json.loads(body)['logical_sha256'] == replay['logical_sha256']
assert json.loads((root / 'stopped-capture/manifest.json').read_text())['state'] == 'complete'
print(json.dumps({'status': 'passed', 'logical_sha256': replay['logical_sha256'], 'revision': replay['final_revision'], 'record_sigterm': 'sealed', 'snapshot': 'namespace-label', 'default_uid': 65532}))
PY
printf 'PASS: image commands, isolated recording/replay, metadata, Web API, TUI, inspector and graceful stop\n'
