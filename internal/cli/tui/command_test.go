package tuicmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yckao/etcd-trend/internal/analysis"
	"github.com/yckao/etcd-trend/internal/cli/stdio"
)

func TestHelpDoesNotRequireTTYOrRun(t *testing.T) {
	var stderr bytes.Buffer
	err := Run(context.Background(), []string{"--help"}, stdio.Streams{In: os.Stdin, Out: os.Stdout, Err: &stderr})
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(stderr.String(), "snapshot") {
		t.Fatalf("help failed: %v %s", err, stderr.String())
	}
}

func TestRejectsMissingRunAndInvalidDimensions(t *testing.T) {
	for _, args := range [][]string{nil, {"--run", "unused", "--width", "1"}, {"--run", "unused", "--height", "10000"}} {
		if err := Run(context.Background(), args, stdio.Streams{In: os.Stdin, Out: os.Stdout, Err: &bytes.Buffer{}}); err == nil {
			t.Fatal("acceptedinvalidflags")
		}
	}
}

func TestTenantSnapshotMatchesExplicitEmptyLabel(t *testing.T) {
	root := t.TempDir()
	a, err := analysis.New(analysis.Config{OutputDir: filepath.Join(root, "analysis"), Window: time.Second, LabelKeys: []string{"tenant"}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Start(analysis.Commit{DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	for i, label := range []string{"", "beta"} {
		name := []string{"empty", "other"}[i]
		value, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "namespace": "ns", "labels": map[string]string{"tenant": label}}})
		if err = a.Event(analysis.Event{Kind: "put", Key: []byte("/registry/configmaps/ns/" + name), Value: value, ValueBytes: int64(len(value)), RecordBytes: int64(len(value) + 32), Revision: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Commit(analysis.Commit{Number: 1, Index: 2, DBSize: 4096, DBSizeInUse: 2048}); err != nil {
		t.Fatal(err)
	}
	summary, err := a.Finish(int64(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	report, _ := json.Marshal(map[string]any{"analysis_directory": "analysis", "analysis": summary})
	if err = os.WriteFile(filepath.Join(root, "report.json"), report, 0600); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(t.TempDir(), "snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	err = Run(context.Background(), []string{"--snapshot", "--run", root, "--scope", "object", "--namespace", "ns", "--label-key", "tenant", "--label-value", ""}, stdio.Streams{In: os.Stdin, Out: out, Err: &bytes.Buffer{}})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(out.Name())
	if !strings.Contains(string(data), "/registry/configmaps/ns/empty") || strings.Contains(string(data), "/registry/configmaps/ns/other") {
		t.Fatalf("empty label filter failed: %s", data)
	}
}
