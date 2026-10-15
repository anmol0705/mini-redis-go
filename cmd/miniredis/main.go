// Command miniredis runs the mini-redis-go server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/anmol0705/mini-redis-go/internal/app"
	"github.com/anmol0705/mini-redis-go/internal/server"
)

// version is reported by INFO.
const version = "0.2.0"

func main() {
	// SIGINT is Ctrl+C. SIGTERM is what `kill` and service managers send.
	// Both names compile on Windows too, where only Ctrl+C is delivered.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	stop() // os.Exit skips deferred calls, so release the signal handler first
	if err != nil {
		fmt.Fprintln(os.Stderr, "miniredis:", err)
		os.Exit(1)
	}
}

// run parses flags, starts the server and blocks until ctx is cancelled and
// the server has shut down. Keeping the real work out of main makes it
// testable: a test passes its own context and arguments.
func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("miniredis", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":6379", "TCP address to listen on")
	maxClients := fs.Int("max-clients", server.DefaultMaxClients, "maximum simultaneous client connections")
	idleTimeout := fs.Duration("idle-timeout", 0, "close clients idle for this long (0 disables)")
	grace := fs.Duration("shutdown-grace", server.DefaultShutdownGrace, "how long to wait for clients on shutdown")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	maxMemory := fs.String("maxmemory", "0", "memory limit as an estimate, e.g. 100mb (0 = unlimited)")
	policy := fs.String("maxmemory-policy", "noeviction", "noeviction, allkeys-lru, allkeys-random or volatile-ttl")
	samples := fs.Int("maxmemory-samples", 5, "keys sampled per eviction")
	queue := fs.Int("queue-size", 1024, "engine request queue capacity")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *maxClients < 1 {
		return errors.New("-max-clients must be at least 1")
	}
	if *idleTimeout < 0 || *grace < 0 {
		return errors.New("-idle-timeout and -shutdown-grace must not be negative")
	}
	if *samples < 1 || *queue < 1 {
		return errors.New("-maxmemory-samples and -queue-size must be at least 1")
	}
	memBytes, err := parseSize(*maxMemory)
	if err != nil {
		return fmt.Errorf("invalid -maxmemory: %w", err)
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", *logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	return app.Run(ctx, app.Config{
		Addr:             *addr,
		MaxClients:       *maxClients,
		IdleTimeout:      *idleTimeout,
		ShutdownGrace:    *grace,
		Version:          version,
		Logger:           logger,
		QueueSize:        *queue,
		MaxMemory:        memBytes,
		MaxMemoryPolicy:  *policy,
		MaxMemorySamples: *samples,
	})
}

// parseSize reads a byte count with an optional kb/mb/gb suffix (powers of
// 1024, case-insensitive).
func parseSize(s string) (int64, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	mult := int64(1)
	for suffix, m := range map[string]int64{"kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30} {
		if strings.HasSuffix(s, suffix) {
			s, mult = strings.TrimSuffix(s, suffix), m
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a non-negative size", s)
	}
	if n > (1<<63-1)/mult {
		return 0, errors.New("size too large")
	}
	return n * mult, nil
}
