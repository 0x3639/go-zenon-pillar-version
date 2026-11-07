package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/0x3639/go-zenon-pillar-version/internal/runner"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Printf("zenon-observer/%s upstream=%s\n", runner.Release, runner.UpstreamRevision)
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := runner.Load()
	if err == nil {
		err = runner.Run(ctx, cfg)
	}
	if err != nil {
		slog.Error("observer stopped", "error", err.Error())
		os.Exit(1)
	}
}
