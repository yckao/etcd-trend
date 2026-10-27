#!/bin/sh
# No daemon required: verify aliases, argument boundaries and signal-friendly exec.
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
export TEST_LOG="$test_dir/args"
for binary in recorder analyzer etcd-trend-web etcd-trend-tui etcd-trend-inspect etcd-trend-demo etcd-trend-loadtest etcd-trend-compat; do
    cat > "$test_dir/$binary" <<'STUB'
#!/bin/sh
printf '%s\n' "${0##*/}" "$@" > "$TEST_LOG"
STUB
    chmod +x "$test_dir/$binary"
done
PATH="$test_dir:$PATH"
export PATH
for pair in 'record recorder' 'replay analyzer' 'web etcd-trend-web' 'tui etcd-trend-tui' 'inspect etcd-trend-inspect' 'demo etcd-trend-demo' 'compat etcd-trend-compat'; do
    set -- $pair
    sh "$repo/container/entrypoint.sh" "$1" --out 'space in path' --label-key 'example.com/team'
    printf '%s\n' "$2" --out 'space in path' --label-key 'example.com/team' > "$test_dir/expected"
    cmp "$test_dir/expected" "$TEST_LOG"
done
sh "$repo/container/entrypoint.sh" loadtest --duration 5s
printf '%s\n' etcd-trend-loadtest --recorder "$test_dir/recorder" --analyzer "$test_dir/analyzer" --duration 5s > "$test_dir/expected"
cmp "$test_dir/expected" "$TEST_LOG"
sh "$repo/container/entrypoint.sh" help > "$test_dir/help"
if sh "$repo/container/entrypoint.sh" unknown > "$test_dir/unknown" 2>&1; then
    printf 'Unknown command should fail\n' >&2
    exit 1
else
    test "$?" -eq 64
fi
printf 'PASS: container command dispatch and argument preservation\n'
