package follow

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"logfollow/internal/logdir"
)

func setupDir(t *testing.T, runID string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, logdir.RunIDFile), []byte(runID+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logdir.SegmentPath(dir, 1), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func appendLine(t *testing.T, dir string, n int, b string) {
	t.Helper()
	f, err := os.OpenFile(logdir.SegmentPath(dir, n), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(b); err != nil {
		t.Fatal(err)
	}
}

func writeEmpty(path string) error {
	return os.WriteFile(path, nil, 0o644)
}

func removeSeg(path string) error {
	return os.Remove(path)
}

// TestSlowConsumerFatal proves the bounded-buffer contract without TCP: with
// buffer B and a subscriber that never reads, B+1 delivered lines cause Emit
// to block past FullWait and the run to terminate as slow_consumer. Other
// runs have their own sink, so they are structurally independent.
func TestSlowConsumerFatal(t *testing.T) {
	dir := setupDir(t, "run-slow")
	const buffer, fullWait = 4, 80 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sink, fatalCh := Run(ctx, Config{
		Dir: dir, PollInterval: 5 * time.Millisecond, Buffer: buffer, FullWait: fullWait,
	}, logdir.Cursor{}, false)

	// Independent sibling run: it must keep delivering while the slow one is
	// stuck (its sink is a separate bounded queue).
	sink2, fatalCh2 := Run(ctx, Config{
		Dir: dir, PollInterval: 5 * time.Millisecond, Buffer: 64, FullWait: time.Second,
	}, logdir.Cursor{}, false)

	// Never drain sink; fill buffer + grace on the slow subscription.
	const slowLines = buffer + 3
	for i := 0; i < slowLines; i++ {
		appendLine(t, dir, 1, `{"i":0}`+"\n")
	}

	select {
	case fe := <-fatalCh:
		if fe == nil || fe.Kind != logdir.KindSlowConsumer {
			t.Fatalf("fatal = %v, want slow_consumer", fe)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscription was not dropped")
	}

	// The events the slow sink did manage to queue must not exceed the bound.
	drained := 0
	for range sink.Events() {
		drained++
	}
	if drained > buffer {
		t.Fatalf("slow sink delivered %d events, buffer bound was %d", drained, buffer)
	}

	// Sibling continues to receive records written after the drop.
	appendLine(t, dir, 1, `{"i":99}`+"\n")
	gotSibling := 0
	deadline := time.After(2 * time.Second)
	for gotSibling < buffer+4 {
		select {
		case ev, ok := <-sink2.Events():
			if !ok {
				t.Fatal("sibling stream closed unexpectedly")
			}
			if ev.Kind == logdir.KindRecord {
				gotSibling++
			}
		case fe := <-fatalCh2:
			t.Fatalf("sibling got fatal: %v", fe)
		case <-deadline:
			t.Fatalf("sibling only got %d records, want %d", gotSibling, buffer+4)
		}
	}
}

// TestContextCancelStopsQuietly verifies cancellation ends the run with no
// fatal reason and closes the event channel (per-connection cancellation).
func TestContextCancelStopsQuietly(t *testing.T) {
	dir := setupDir(t, "run-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	sink, fatalCh := Run(ctx, Config{Dir: dir, PollInterval: 5 * time.Millisecond}, logdir.Cursor{}, false)

	appendLine(t, dir, 1, `{"x":1}`+"\n")
	select {
	case <-sink.Events():
	case <-time.After(time.Second):
		t.Fatal("first event never arrived")
	}
	cancel()
	select {
	case fe, ok := <-fatalCh:
		if ok {
			t.Fatalf("cancellation produced fatal: %v", fe)
		}
	case <-time.After(time.Second):
		t.Fatal("fatal channel did not close after cancel")
	}
	if _, ok := <-sink.Events(); ok {
		t.Fatal("events channel should be closed after cancel")
	}
}
