package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"logfollower/internal/follower"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func atoi(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Fatalf("%s must be a positive integer, got %q", key, v)
	}
	return n
}

func generateRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		log.Fatalf("generate run id: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func main() {
	dir := getenv("LOG_DIR", "/var/log/segments")
	runID := os.Getenv("LOG_RUN_ID")
	if runID == "" {
		runID = generateRunID()
		log.Printf("LOG_RUN_ID not set; generated run id %s", runID)
	}
	pollMs := atoi("POLL_INTERVAL_MS", 20)
	buffer := atoi("EVENT_BUFFER", 64)
	addr := getenv("LISTEN_ADDR", ":8080")
	barrier := follower.NewBarrier()

	f, err := follower.New(follower.Config{
		Directory:    dir,
		RunID:        runID,
		PollInterval: time.Duration(pollMs) * time.Millisecond,
		EventBuffer:  buffer,
		Barrier:      barrier,
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	server := &follower.Server{Follow: f, Barrier: barrier}
	server.Routes(mux)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("log follower listening on %s for %s with run id %s", addr, dir, runID)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
