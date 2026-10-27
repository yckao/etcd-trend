package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"testing"
)

func TestHelpDoesNotOpenRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &stdout, &stderr); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
	if stderr.Len() == 0 || stdout.Len() != 0 {
		t.Fatal("help output missing")
	}
}

func TestInvalidWindow(t *testing.T) {
	for _, value := range []string{"-1", "1.2", "later"} {
		var stdout, stderr bytes.Buffer
		if err := run(context.Background(), []string{"--run", "missing", "--window", value}, &stdout, &stderr); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
