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
	"syscall"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/server"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// version is reported by INFO.
const version = "0.1.0"

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

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", *logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	srv, err := server.Listen(server.Config{
		Addr:          *addr,
		MaxClients:    *maxClients,
		IdleTimeout:   *idleTimeout,
		ShutdownGrace: *grace,
		Version:       version,
		Logger:        logger,
	})
	if err != nil {
		return err
	}
	logger.Info("listening", "addr", srv.Addr().String(), "version", version)

	// The server supplies live numbers (clients, uptime) to INFO through
	// the command.InfoSource interface, which *server.Server satisfies.
	dispatcher := command.New(store.New(), srv)
	if err := srv.Serve(ctx, dispatcher); err != nil {
		return err
	}
	logger.Info("stopped")
	return nil
}

// Compile-time proof that the server can feed INFO.
var _ command.InfoSource = (*server.Server)(nil)

// Compile-time proof that the dispatcher can serve commands.
var _ server.Handler = (*command.Dispatcher)(nil)
