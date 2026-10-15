package main

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const testTimeout = 10 * time.Second

// syncBuffer is a bytes.Buffer that is safe to write from the server's
// goroutines while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var listeningRE = regexp.MustCompile(`msg=listening addr=(\S+)`)

func TestRunServesAndShutsDownOnCancel(t *testing.T) {
	var stderr syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0"}, &stderr) }()

	// Learn the ephemeral port from the "listening" log line.
	var addr string
	deadline := time.After(testTimeout)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for addr == "" {
		select {
		case err := <-done:
			t.Fatalf("run returned early: %v\n%s", err, stderr.String())
		case <-deadline:
			t.Fatalf("server never logged its address:\n%s", stderr.String())
		case <-tick.C:
			if m := listeningRE.FindStringSubmatch(stderr.String()); m != nil {
				addr = m[1]
			}
		}
	}

	conn, err := net.DialTimeout("tcp", addr, testTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(testTimeout))
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || line != "+PONG\r\n" {
		t.Fatalf("reply = %q, %v; want +PONG", line, err)
	}

	cancel() // what Ctrl+C or SIGTERM does via signal.NotifyContext
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run returned %v after a graceful shutdown; want nil", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("run did not return after the context was cancelled")
	}
	if !strings.Contains(stderr.String(), "msg=stopped") {
		t.Errorf("no 'stopped' log line:\n%s", stderr.String())
	}
}

func TestRunRejectsBadFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"-nope"}},
		{"zero max clients", []string{"-max-clients", "0"}},
		{"negative idle timeout", []string{"-idle-timeout", "-1s"}},
		{"bad log level", []string{"-log-level", "shouty"}},
		{"stray argument", []string{"extra"}},
		{"unusable address", []string{"-addr", "not an address"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr syncBuffer
			if err := run(context.Background(), tt.args, &stderr); err == nil {
				t.Errorf("run(%v) succeeded; want an error", tt.args)
			}
		})
	}
}

func TestRunHelpIsNotAnError(t *testing.T) {
	var stderr syncBuffer
	if err := run(context.Background(), []string{"-h"}, &stderr); err != nil {
		t.Errorf("run -h returned %v", err)
	}
	if !strings.Contains(stderr.String(), "-addr") {
		t.Errorf("help text lacks -addr:\n%s", stderr.String())
	}
}

func TestParseSize(t *testing.T) {
	good := map[string]int64{"0": 0, "1024": 1024, "1kb": 1024, "2MB": 2 << 20, " 3 gb ": 3 << 30}
	for in, want := range good {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "-1", "abc", "1tb", "99999999999gb"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) succeeded; want an error", in)
		}
	}
}
