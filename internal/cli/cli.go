// Package cli dispatches every etcd-trend command within one executable.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"runtime/debug"

	compatcmd "github.com/yckao/etcd-trend/internal/cli/compat"
	democmd "github.com/yckao/etcd-trend/internal/cli/demo"
	inspectcmd "github.com/yckao/etcd-trend/internal/cli/inspect"
	loadtestcmd "github.com/yckao/etcd-trend/internal/cli/loadtest"
	recordcmd "github.com/yckao/etcd-trend/internal/cli/record"
	replaycmd "github.com/yckao/etcd-trend/internal/cli/replay"
	"github.com/yckao/etcd-trend/internal/cli/stdio"
	tuicmd "github.com/yckao/etcd-trend/internal/cli/tui"
	webcmd "github.com/yckao/etcd-trend/internal/cli/web"
)

type Streams = stdio.Streams
type command func(context.Context, []string, stdio.Streams) error

func resolve(name string) (command, bool) {
	switch name {
	case "record", "recorder":
		return recordcmd.Run, true
	case "replay", "analyzer":
		return replaycmd.Run, true
	case "web", "gui":
		return webcmd.Run, true
	case "tui":
		return tuicmd.Run, true
	case "inspect":
		return inspectcmd.Run, true
	case "demo":
		return democmd.Run, true
	case "loadtest":
		return loadtestcmd.Run, true
	case "compat":
		return compatcmd.Run, true
	default:
		return nil, false
	}
}

// Run executes a command without changing process-global arguments or streams.
func Run(ctx context.Context, args []string, streams Streams) error {
	streams = streams.WithDefaults()
	if len(args) == 0 {
		return help(streams.Out)
	}
	switch args[0] {
	case "help", "--help", "-h":
		if len(args) == 1 {
			return help(streams.Out)
		}
		if args[0] != "help" || len(args) != 2 {
			return stdio.UsageError{Err: fmt.Errorf("usage: etcd-trend help [command]")}
		}
		cmd, ok := resolve(args[1])
		if !ok {
			return unknown(args[1])
		}
		return cmd(ctx, []string{"--help"}, streams)
	case "version", "--version":
		if len(args) != 1 {
			return stdio.UsageError{Err: fmt.Errorf("usage: etcd-trend version")}
		}
		_, err := fmt.Fprintln(streams.Out, version())
		return err
	}
	cmd, ok := resolve(args[0])
	if !ok {
		return unknown(args[0])
	}
	return cmd(ctx, args[1:], streams)
}

func unknown(name string) error {
	return stdio.UsageError{Err: fmt.Errorf("unknown command %q; run 'etcd-trend help'", name)}
}

// Execute returns the process exit code, printing errors only to the error stream.
func Execute(ctx context.Context, args []string, streams Streams) int {
	streams = streams.WithDefaults()
	err := Run(ctx, args, streams)
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintln(streams.Err, "etcd-trend:", err)
	var usage stdio.UsageError
	if errors.As(err, &usage) {
		return 2
	}
	return 1
}

func help(out io.Writer) error {
	_, err := fmt.Fprintln(out, `Usage: etcd-trend <command> [options]

Commands:
  record    Preserve an existing member's snapshot and WAL files offline
  replay    Replay a capture and analyze database/object changes
  web       Serve the offline Web explorer on loopback
  tui       Open the offline terminal explorer or print a snapshot
  inspect   Print stored analysis rankings
  demo      Run an isolated synthetic recording/replay demonstration
  loadtest  Measure this executable's record/replay child processes
  compat    Verify an isolated external etcd release fixture
  version   Print build information

Aliases: recorder=record, analyzer=replay, gui=web
Use 'etcd-trend help <command>' or '<command> --help' for options.

`+stdio.Warning)
	return err
}

func version() string {
	value := "etcd-trend (devel)"
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" {
			value = "etcd-trend " + info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				value += " " + setting.Value
			}
		}
	}
	return value
}
