//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !dragonfly

package tui

import (
	"context"
	"fmt"
)

func readKeys(context.Context, int, chan<- string) error {
	return fmt.Errorf("interactive terminal input is supported on Unix hosts; use --snapshot")
}
