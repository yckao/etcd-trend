//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly

package tui

import (
	"context"
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

func readKeys(ctx context.Context, fd int, out chan<- string) error {
	var decoder Decoder
	buffer := make([]byte, 128)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, 80)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		var keys []string
		if n == 0 {
			keys = decoder.Feed(nil, true)
		} else {
			if fds[0].Revents&unix.POLLNVAL != 0 {
				return io.EOF
			}
			n, err = unix.Read(fd, buffer)
			if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
				continue
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.EOF
			}
			keys = decoder.Feed(buffer[:n], false)
		}
		for _, key := range keys {
			select {
			case out <- key:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
