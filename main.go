package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/yckao/etcd-trend/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx, os.Args[1:], cli.Streams{In: os.Stdin, Out: os.Stdout, Err: os.Stderr})
	stop()
	if code != 0 {
		os.Exit(code)
	}
}
