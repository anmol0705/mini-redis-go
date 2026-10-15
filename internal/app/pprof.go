package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// pprofServer serves the net/http/pprof handlers on a loopback-only socket.
// It is off unless -pprof-addr is given, and it refuses non-loopback
// addresses: profiling endpoints reveal internals and can be used to burn CPU,
// so they must never be reachable from the network.
//
// Concurrency: one goroutine runs http.Server.Serve; stop shuts the server
// down and waits for that goroutine.
type pprofServer struct {
	ln   net.Listener
	srv  *http.Server
	done chan struct{}
}

// checkLoopback returns an error unless addr names a loopback host.
func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("%q is not a loopback address; pprof is only served on localhost (use 127.0.0.1:PORT or [::1]:PORT)", addr)
}

func startPprof(addr string, log *slog.Logger) (*pprofServer, error) {
	if err := checkLoopback(addr); err != nil {
		return nil, fmt.Errorf("pprof: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof: %w", err)
	}
	// A private mux: importing net/http/pprof also registers on the default
	// mux, which nothing else here serves.
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	p := &pprofServer{
		ln:   ln,
		srv:  &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		done: make(chan struct{}),
	}
	go func() {
		defer close(p.done)
		if err := p.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("pprof server failed", "err", err)
		}
	}()
	log.Info("pprof listening", "addr", ln.Addr().String())
	return p, nil
}

func (p *pprofServer) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.srv.Shutdown(ctx); err != nil {
		_ = p.srv.Close() // a profile download may still be running; cut it
	}
	<-p.done
}
