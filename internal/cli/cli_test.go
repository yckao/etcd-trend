package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/cli/stdio"
	"github.com/yckao/etcd-trend/internal/loadtest"
)

// The self-exec test runs the same test executable as record/replay children.
// Dispatch only explicitly marked children; ordinary test invocations use m.Run.
func TestMain(m *testing.M) {
	if os.Getenv("ETCD_TREND_CLI_TEST_CHILD") == "1" && len(os.Args) > 1 && (os.Args[1] == "record" || os.Args[1] == "replay") {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		code := Execute(ctx, os.Args[1:], Streams{In: os.Stdin, Out: os.Stdout, Err: os.Stderr})
		stop()
		os.Exit(code)
	}
	os.Exit(m.Run())
}

func TestCommandHelpRoutesWithoutOpeningData(t *testing.T) {
	options := map[string]string{
		"record": "-wal-dir", "recorder": "-wal-dir",
		"replay": "-capture", "analyzer": "-capture",
		"web": "-listen", "gui": "-listen", "tui": "-snapshot",
		"inspect": "-window", "demo": "-updates",
		"loadtest": "-bytes-per-second", "compat": "-etcd-binary",
	}
	original := append([]string(nil), os.Args...)
	for name, option := range options {
		for _, args := range [][]string{{name, "--help"}, {"help", name}} {
			var out, stderr bytes.Buffer
			if code := Execute(context.Background(), args, Streams{Out: &out, Err: &stderr}); code != 0 {
				t.Fatalf("%q exit=%d stderr=%s", args, code, stderr.String())
			}
			if out.Len() != 0 || !strings.Contains(stderr.String(), option) || !strings.Contains(stderr.String(), stdio.Warning) {
				t.Fatalf("%q help missing expected option/warning: stdout=%q stderr=%q", args, out.String(), stderr.String())
			}
		}
	}
	if !reflect.DeepEqual(os.Args, original) {
		t.Fatal("dispatch changed process arguments")
	}
}

func TestRootHelpVersionAndErrorExitCodes(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"--help"}, {"-h"}} {
		var out, stderr bytes.Buffer
		if code := Execute(context.Background(), args, Streams{Out: &out, Err: &stderr}); code != 0 || !strings.Contains(out.String(), stdio.Warning) || stderr.Len() != 0 {
			t.Fatalf("root help %q exit=%d stdout=%q stderr=%q", args, code, out.String(), stderr.String())
		}
	}
	for _, args := range [][]string{{"version"}, {"--version"}} {
		var out, stderr bytes.Buffer
		if code := Execute(context.Background(), args, Streams{Out: &out, Err: &stderr}); code != 0 || !strings.HasPrefix(out.String(), "etcd-trend ") || strings.Contains(out.String(), stdio.Warning) || stderr.Len() != 0 {
			t.Fatalf("version %q exit=%d stdout=%q stderr=%q", args, code, out.String(), stderr.String())
		}
	}
	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"unknown"}, 2}, {[]string{"help", "unknown"}, 2},
		{[]string{"--help", "extra"}, 2}, {[]string{"version", "extra"}, 2},
		{[]string{"record"}, 1}, {[]string{"replay"}, 1}, {[]string{"web"}, 1},
		{[]string{"tui"}, 1}, {[]string{"inspect"}, 1}, {[]string{"compat"}, 1},
		{[]string{"demo", "--updates", "0"}, 1}, {[]string{"loadtest"}, 2},
		{[]string{"loadtest", "--bad-flag"}, 2},
	} {
		var out, stderr bytes.Buffer
		if code := Execute(context.Background(), test.args, Streams{Out: &out, Err: &stderr}); code != test.code || out.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("%q exit=%d want=%d stdout=%q stderr=%q", test.args, code, test.code, out.String(), stderr.String())
		}
	}
}

func TestLoadtestUsesSameExecutableForRecordAndReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an isolated loopback fixture and two child processes")
	}
	t.Setenv("ETCD_TREND_CLI_TEST_CHILD", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dir := filepath.Join(t.TempDir(), "run")
	var out, stderr bytes.Buffer
	code := Execute(ctx, []string{"loadtest", "--out", dir, "--duration", "250ms", "--bytes-per-second", "131072", "--value-bytes", "4096", "--keys", "3", "--compaction-interval", "100ms"}, Streams{Out: &out, Err: &stderr})
	if code != 0 {
		for _, name := range []string{"recorder.stderr.log", "analyzer.stderr.log"} {
			data, _ := os.ReadFile(filepath.Join(dir, name))
			t.Logf("%s: %s", name, data)
		}
		t.Fatalf("loadtest exit=%d stderr=%s", code, stderr.String())
	}
	var summary loadtest.Summary
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("stdout must remain one JSON value: %v\n%s", err, out.String())
	}
	if summary.Updates == 0 || summary.ExpectedRevision != summary.ActualRevision || summary.ExpectedSHA256 == "" || summary.ExpectedSHA256 != summary.ActualSHA256 || summary.OperationErrors != 0 {
		t.Fatalf("self-exec replay did not reproduce source: %+v", summary)
	}
	for _, name := range []string{"recorder.stdout.json", "analyzer.stdout.json"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !json.Valid(data) || bytes.Contains(data, []byte(stdio.Warning)) {
			t.Fatalf("%s must contain JSON only: %v %s", name, err, data)
		}
	}
}
