package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/runner"
	"github.com/0x3639/go-zenon-pillar-version/internal/storage"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 {
			os.Exit(1)
		}
		c := http.Client{Timeout: 2 * time.Second}
		r, e := c.Get("http://127.0.0.1:" + port + "/healthz")
		if e != nil {
			os.Exit(1)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 3 && os.Args[1] == "backup" {
		dir := os.Getenv("DATA_DIR")
		if dir == "" {
			dir = "/data"
		}
		budget := int64(2048)
		if v := os.Getenv("DATA_BUDGET_MIB"); v != "" {
			var e error
			budget, e = strconv.ParseInt(v, 10, 64)
			if e != nil || budget < 1024 || budget > 2048 {
				fmt.Fprintln(os.Stderr, "invalid DATA_BUDGET_MIB")
				os.Exit(1)
			}
		}
		ctx, end := context.WithTimeout(context.Background(), time.Minute)
		defer end()
		if e := storage.Backup(ctx, dir, os.Args[2], budget<<20); e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		fmt.Println("consistent SQLite backup created:", os.Args[2])
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Printf("zenon-observer/%s upstream=%s\n", runner.Release, runner.UpstreamRevision)
		return
	}
	if len(os.Args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: observer [version | healthcheck | backup <new-name.sqlite>]")
		os.Exit(2)
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
