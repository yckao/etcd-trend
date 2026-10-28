package replaycmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOutputCannotModifyCapture(t *testing.T) {
	root := t.TempDir()
	capture := filepath.Join(root, "capture")
	if err := os.Mkdir(capture, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(capture, alias); err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{capture, filepath.Join(capture, "run"), filepath.Join(alias, "run")} {
		if err := validateOutput(capture, out); err == nil {
			t.Fatalf("accepted output inside capture: %s", out)
		}
	}
	if err := validateOutput(capture, filepath.Join(root, "run")); err != nil {
		t.Fatal(err)
	}
}

func TestIndexCannotOccupyReplayOutput(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "run")
	for _, index := range []string{out, filepath.Join(alias, "run")} {
		if err := validateSeparatePaths(out, index); err == nil {
			t.Fatalf("accepted conflicting index path: %s", index)
		}
	}
	if err := validateSeparatePaths(out, filepath.Join(root, "index")); err != nil {
		t.Fatal(err)
	}
}
