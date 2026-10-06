// Command logfollow serves the NDJSON log-following SSE endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"logfollow/internal/stream"
)

func main() {
	addr := flag.String("addr", envOr("LOGFOLLOW_ADDR", ":8080"), "listen address")
	dir := flag.String("dir", envOr("LOGFOLLOW_DIR", "/logs"), "log directory")
	poll := flag.Duration("poll", durationEnv("LOGFOLLOW_POLL", 30*time.Millisecond), "directory poll interval")
	buffer := flag.Int("buffer", intEnv("LOGFOLLOW_BUFFER", 256), "per-connection event buffer")
	fullWait := flag.Duration("full-wait", durationEnv("LOGFOLLOW_FULL_WAIT", 2*time.Second), "grace before dropping a slow subscriber")
	heartbeat := flag.Duration("heartbeat", durationEnv("LOGFOLLOW_HEARTBEAT", 15*time.Second), "SSE heartbeat interval")
	fatalGrace := flag.Duration("fatal-grace", durationEnv("LOGFOLLOW_FATAL_GRACE", 5*time.Second), "deadline for delivering a terminal frame")
	flag.Parse()

	if fi, err := os.Stat(*dir); err != nil {
		log.Fatalf("log directory %q unavailable: %v", *dir, err)
	} else if !fi.IsDir() {
		log.Fatalf("log directory %q is not a directory", *dir)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           stream.NewServer(stream.Config{Dir: *dir, PollInterval: *poll, Buffer: *buffer, FullWait: *fullWait, Heartbeat: *heartbeat, FatalGrace: *fatalGrace}).Mux(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("logfollow serving %s, dir=%s", *addr, *dir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
		log.Printf("ignoring invalid integer %s=%q", key, v)
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("ignoring invalid duration %s=%q", key, v)
	}
	return fallback
}
