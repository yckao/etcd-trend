#!/bin/sh
set -eu

command=${1:-help}
if [ "$#" -gt 0 ]; then shift; fi
case "$command" in
    record|recorder) exec recorder "$@" ;;
    replay|analyzer) exec analyzer "$@" ;;
    web|gui|etcd-trend-web) exec etcd-trend-web "$@" ;;
    tui|etcd-trend-tui) exec etcd-trend-tui "$@" ;;
    inspect|etcd-trend-inspect) exec etcd-trend-inspect "$@" ;;
    demo|etcd-trend-demo) exec etcd-trend-demo "$@" ;;
    loadtest|etcd-trend-loadtest)
        exec etcd-trend-loadtest --recorder "$(command -v recorder)" --analyzer "$(command -v analyzer)" "$@"
        ;;
    compat|etcd-trend-compat) exec etcd-trend-compat "$@" ;;
    sh|/bin/sh) exec /bin/sh "$@" ;;
    help|--help|-h)
        cat <<'HELP'
Usage: etcd-trend COMMAND [ARGS...]

  record    Record a member's WAL using an explicit consistent baseline
  replay    Replay a completed capture and extract offline analysis
  web       Serve a completed analysis on a loopback address
  tui       Explore a completed analysis in an interactive terminal (-it)
  inspect   Print a selected analysis window
  demo      Run an isolated synthetic etcd demonstration
  loadtest  Record a paced isolated synthetic workload
  compat    Qualify an explicitly supplied etcd release binary
  sh        Run the bundled shell

Pass COMMAND --help for command-specific options.
Mount captures and runs under /data. Default identity is 65532:65532.
HELP
        ;;
    *) printf 'Unknown command: %s\nRun etcd-trend help for commands.\n' "$command" >&2; exit 64 ;;
esac
