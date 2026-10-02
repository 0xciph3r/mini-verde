// Command worker runs one Mini-Verde worker on a loopback HTTP listener.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Printf("worker: %v", err)
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	workerID := flags.String("id", "", "stable worker identifier")
	listenAddress := flags.String("listen", "127.0.0.1:8081", "loopback listen address")
	behavior := flags.String("behavior", string(worker.BehaviorHonest), "worker behavior for deterministic fault injection")
	faultSeed := flags.Uint64("fault-seed", 0, "seed for deterministic fault injection")
	behaviorDelay := flags.Duration("behavior-delay", 0, "delay applied by the slow behavior")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if err := validateLoopbackAddress(*listenAddress); err != nil {
		return err
	}
	config := worker.DefaultConfig()
	config.Limits = protocol.DefaultLimits()
	config.Behavior = worker.Behavior(*behavior)
	config.FaultSeed = *faultSeed
	config.Delay = *behaviorDelay
	if config.Behavior == worker.BehaviorCrasher {
		config.Crash = func() { os.Exit(70) }
	}
	handler, err := worker.NewWithConfig(*workerID, config)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	server := newHTTPServer(handler)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	log.Printf("worker %q listening on http://%s", *workerID, listener.Addr())

	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}

func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 2 * time.Second,
		// Execution duration is governed by the coordinator's configurable
		// request and job contexts. A server write deadline must not truncate a
		// valid job before those deadlines.
		WriteTimeout:   0,
		IdleTimeout:    30 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
}

func validateLoopbackAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen address must be host:port: %w", err)
	}
	if port == "" {
		return fmt.Errorf("listen address requires a port")
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("listen address must use a loopback host")
	}
	return nil
}
