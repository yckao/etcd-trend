package inspectcmd

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"testing"

	"github.com/yckao/etcd-trend/internal/cli/stdio"
)

func TestHelpDoesNotOpenRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), []string{"--help"}, stdio.Streams{Out: &stdout, Err: &stderr}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
	if stderr.Len() == 0 || stdout.Len() != 0 {
		t.Fatal("help output missing")
	}
}

func TestInvalidWindow(t *testing.T) {
	for _, value := range []string{"-1", "1.2", "later"} {
		var stdout, stderr bytes.Buffer
		if err := Run(context.Background(), []string{"--run", "missing", "--window", value}, stdio.Streams{Out: &stdout, Err: &stderr}); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
