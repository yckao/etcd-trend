// Package stdio provides explicit streams and consistent help for CLI commands.
package stdio

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

const Warning = "WARNING: This project is pure vibe coding and has not been manually reviewed."

type Streams struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

func (s Streams) WithDefaults() Streams {
	if s.In == nil {
		s.In = strings.NewReader("")
	}
	if s.Out == nil {
		s.Out = io.Discard
	}
	if s.Err == nil {
		s.Err = io.Discard
	}
	return s
}

func Flags(command string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("etcd-trend "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: etcd-trend %s [options]\n\n", command)
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\n%s\n", Warning)
	}
	return fs
}

// UsageError retains the exit status of commands which previously used flag.ExitOnError.
type UsageError struct{ Err error }

func (e UsageError) Error() string { return e.Err.Error() }
func (e UsageError) Unwrap() error { return e.Err }
