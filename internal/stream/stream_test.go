package stream_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"logfollow/internal/stream"
	"logfollow/internal/testutil"
)

const (
	pollInterval = 5 * time.Millisecond
	eventTimeout = 3 * time.Second
)

func testServer(t *testing.T, dir string, opts ...func(*stream.Config)) *httptest.Server {
	t.Helper()
	cfg := stream.Config{
		Dir:          dir,
		PollInterval: pollInterval,
		Buffer:       256,
		FullWait:     time.Second,
		Heartbeat:    time.Hour, // keep test frames deterministic
		FatalGrace:   2 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	srv := httptest.NewServer(stream.NewServer(cfg).Mux())
	t.Cleanup(srv.Close)
	return srv
}

func newProducer(t *testing.T, runID string) (*testutil.Producer, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "logs")
	p, err := testutil.NewProducer(dir, runID)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	return p, dir
}

// TestByteOffsetsHalfUTF8AndBadJSON verifies byte-accurate offsets (not rune
// counts), waiting on half lines and split UTF-8 sequences, and that a bad
// JSON line is delivered as an error event without swallowing later lines.
func TestByteOffsetsHalfUTF8AndBadJSON(t *testing.T) {
	p, dir := newProducer(t, "run-A")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	// First line contains a multibyte rune (世 = E4 B8 96).
	line1 := []byte(`{"msg":"世界-1"}` + "\n")
	if err := p.Append(line1); err != nil {
		t.Fatal(err)
	}
	// Offset is measured in bytes: line1 has more runes than bytes.
	ev1, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait line1: %v", err)
	}
	if off := testutil.RequireIntField(t, ev1, "offset"); off != int64(len(line1)) {
		t.Fatalf("offset = %d, want %d (byte boundary, not rune count)", off, len(line1))
	}
	if seg := testutil.RequireIntField(t, ev1, "segment"); seg != 1 {
		t.Fatalf("segment = %d, want 1", seg)
	}
	run, _ := ev1.Field("run")
	if run != "run-A" {
		t.Fatalf("run = %v, want run-A", run)
	}

	// Half line: must NOT be delivered yet.
	if err := p.Append([]byte(`{"partial":tr`)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 150*time.Millisecond); len(got) != 0 {
		t.Fatalf("half line emitted early: %+v", got)
	}
	// Complete it.
	if err := p.Append([]byte(`ue}` + "\n")); err != nil {
		t.Fatal(err)
	}
	ev2, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait completed partial: %v", err)
	}
	// Offset covers line1 plus the full 18-byte second line.
	wantOff := int64(len(line1)) + int64(len(`{"partial":true}`)+1)
	if off := testutil.RequireIntField(t, ev2, "offset"); off != wantOff {
		t.Fatalf("offset after partial = %d, want %d", off, wantOff)
	}

	// Split a multibyte UTF-8 sequence across two appends:
	// "中" = E4 B8 AD. Bytes without newline must wait; then one clean record.
	if err := p.Append([]byte(`{"c":"`)); err != nil {
		t.Fatal(err)
	}
	if err := p.Append([]byte{0xe4, 0xb8}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 150*time.Millisecond); len(got) != 0 {
		t.Fatalf("split UTF-8 emitted early: %+v", got)
	}
	if err := p.Append([]byte{0xad, '"', '}', '\n'}); err != nil {
		t.Fatal(err)
	}
	ev3, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait utf8: %v", err)
	}
	line3 := []byte(`{"c":"中"}` + "\n")
	wantOff3 := wantOff + int64(len(line3))
	if off := testutil.RequireIntField(t, ev3, "offset"); off != wantOff3 {
		t.Fatalf("utf8 offset = %d, want %d", off, wantOff3)
	}

	// A syntactically broken complete line -> bad_json, then a good line
	// must still arrive (the error does not consume the stream).
	if err := p.AppendLine([]byte(`{"oops":`)); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(42, "after-bad"); err != nil {
		t.Fatal(err)
	}
	bad, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "bad_json"
	})
	if err != nil {
		t.Fatalf("wait bad_json: %v", err)
	}
	if line, _ := bad.Field("line"); line != `{"oops":` {
		t.Fatalf("bad line = %v, want %q", line, `{"oops":`)
	}
	good, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait line after bad: %v", err)
	}
	rec, _ := good.Field("record")
	recMap, _ := rec.(map[string]any)
	if recMap["msg"] != "after-bad" {
		t.Fatalf("record after bad_json = %v", good.Data)
	}
	if seg := testutil.RequireIntField(t, good, "segment"); seg != 1 {
		t.Fatalf("segment = %d, want 1", seg)
	}
}
