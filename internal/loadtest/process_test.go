package loadtest

import (
	"context"
	"os"
	"reflect"
	"testing"
)

func TestChildCommandDefaultsToCurrentExecutable(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"record", "replay"} {
		cmd, err := childCommand(context.Background(), "", name, "--out", "a path with spaces")
		if err != nil {
			t.Fatal(err)
		}
		want := []string{executable, name, "--out", "a path with spaces"}
		if cmd.Path != executable || !reflect.DeepEqual(cmd.Args, want) {
			t.Fatalf("self-exec: path=%q args=%q, want %q", cmd.Path, cmd.Args, want)
		}
	}
}

func TestChildCommandPreservesExplicitLegacyExecutable(t *testing.T) {
	cmd, err := childCommand(context.Background(), "/custom/recorder", "record", "--out", "run")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"/custom/recorder", "--out", "run"}; !reflect.DeepEqual(cmd.Args, want) {
		t.Fatalf("legacy executable must not receive a subcommand: %q", cmd.Args)
	}
}
