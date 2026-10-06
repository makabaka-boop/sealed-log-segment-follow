package stream_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"logfollow/internal/logdir"
	"logfollow/internal/testutil"
)

// fixedLine renders a deterministic N-byte NDJSON line.
func fixedLine(tag string, n int) []byte {
	prefix := `{"l":"` + tag + `","p":"`
	suffix := `"}`
	const nl = 1
	fill := n - len(prefix) - len(suffix) - nl
	if fill < 0 {
		panic("line too short")
	}
	return []byte(prefix + strings.Repeat("x", fill) + suffix + "\n")
}

func init() {
	// Sanity guard for the fixture arithmetic.
	if l := len(fixedLine("a", 4200)); l != 4200 {
		panic("fixedLine length bug")
	}
}

// TestSegmentBoundaryNoSplice: bytes split by a rollover must never be glued
// into one record; the segment-0002 first line carries segment=2 and the
// correct per-segment byte offset.
func TestSegmentBoundaryNoSplice(t *testing.T) {
	p, dir := newProducer(t, "run-boundary")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two 4200-byte lines (8400 combined > 8192): the second crosses the
	// 8 KiB limit, so the producer rolls and it lands in segment-0002.
	l1 := fixedLine("a", 4200)
	if err := p.AppendLineWithRoll(l1); err != nil {
		t.Fatal(err)
	}
	l2 := fixedLine("b", 4200)
	if err := p.AppendLineWithRoll(l2); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(p.Path(1)); err != nil || fi.Size() != 4200 {
		t.Fatalf("segment 1 size = %v (err=%v), want 4200", fi, err)
	}
	if fi, err := os.Stat(p.Path(2)); err != nil || fi.Size() != 4200 {
		t.Fatalf("segment 2 size = %v (err=%v), want 4200", fi, err)
	}

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	evA, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait a: %v", err)
	}
	if seg := testutil.RequireIntField(t, evA, "segment"); seg != 1 {
		t.Fatalf("a segment = %d want 1", seg)
	}
	if off := testutil.RequireIntField(t, evA, "offset"); off != 4200 {
		t.Fatalf("a offset = %d want 4200", off)
	}
	evB, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait b: %v", err)
	}
	if seg := testutil.RequireIntField(t, evB, "segment"); seg != 2 {
		t.Fatalf("b segment = %d want 2 (rollover crossed)", seg)
	}
	if off := testutil.RequireIntField(t, evB, "offset"); off != 4200 {
		t.Fatalf("b offset = %d want 4200 (per-segment offset)", off)
	}
	// Event id must decode to exactly the reported position.
	dec, err := logdir.DecodeCursor(evB.ID)
	if err != nil {
		t.Fatalf("decode id: %v", err)
	}
	if dec.Segment != 2 || dec.Offset != 4200 || dec.RunID != "run-boundary" {
		t.Fatalf("id cursor = %+v", dec)
	}

	// Append a half-line to the now-active segment while connected: it must
	// wait rather than be emitted. Assert quiet strictly within the window
	// before completing the line (a drain that overlaps completion would
	// steal the legitimate final event).
	if err := p.Append([]byte(`{"wait":1`)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 120*time.Millisecond); len(got) != 0 {
		t.Fatalf("half line leaked: %+v", got)
	}
	if err := p.Append([]byte("}\n")); err != nil {
		t.Fatal(err)
	}
	evC, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait c: %v", err)
	}
	if seg := testutil.RequireIntField(t, evC, "segment"); seg != 2 {
		t.Fatalf("c segment = %d want 2", seg)
	}
	if off := testutil.RequireIntField(t, evC, "offset"); off != 4200+int64(len(`{"wait":1}`))+1 {
		t.Fatalf("c offset = %d", off)
	}
}

// TestTruncatedSealedReportsAndStops: a sealed segment missing its trailing
// newline is evidence loss. The running subscriber gets a fatal truncated
// event and stops; a resume into that segment is rejected explicitly.
func TestTruncatedSealedReportsAndStops(t *testing.T) {
	p, dir := newProducer(t, "run-trunc")
	srv := testServer(t, dir)

	if err := p.AppendJSON(1, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := p.Append([]byte(`{"dangling":true`)); err != nil { // no newline
		t.Fatal(err)
	}
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(2, "seg2"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	// First the complete record is delivered normally.
	ev1, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait ev1: %v", err)
	}
	fatal, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "fatal"
	})
	if err != nil {
		t.Fatalf("wait fatal: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(fatal.Data, &body); err != nil {
		t.Fatalf("fatal data: %v", err)
	}
	if body["kind"] != logdir.KindTruncated {
		t.Fatalf("fatal kind = %v want truncated", body["kind"])
	}
	cur, _ := body["cursor"].(map[string]any)
	if seg, _ := cur["seg"].(float64); int(seg) != 1 {
		t.Fatalf("fatal cursor = %v", body["cursor"])
	}
	// Channel must have closed (subscription truly stopped), and segment-0002
	// records must never have been spliced after the dangling bytes.
	if _, ok := <-cli.Events(); ok {
		t.Fatalf("expected stream close after truncated fatal")
	}

	// A resume at the last good boundary reaches the same evidence: the
	// boundary itself is valid, so preflight accepts, then the follower hits
	// the missing trailing newline and terminates that subscription.
	cli2 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli2.Connect(ctx, ev1.ID); err != nil {
		t.Fatalf("resume preflight: %v", err)
	}
	defer cli2.Close()
	fatal2, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "fatal"
	})
	if err != nil {
		t.Fatalf("wait fatal2: %v", err)
	}
	var body2 map[string]any
	_ = json.Unmarshal(fatal2.Data, &body2)
	if body2["kind"] != logdir.KindTruncated {
		t.Fatalf("fatal2 kind = %v", body2["kind"])
	}
}

// TestResumeAcrossRollover uses Last-Event-ID to reconnect after a rollover
// and asserts no record is replayed or lost.
func TestResumeAcrossRollover(t *testing.T) {
	p, dir := newProducer(t, "run-resume")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := p.AppendJSON(1, "first"); err != nil {
		t.Fatal(err)
	}
	ev1, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatal(err)
	}
	// Client disconnects with ev1's id as Last-Event-ID.
	cli.Close()

	// Producer rolls while the client is offline, then writes two lines.
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(2, "seg2-a"); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(3, "seg2-b"); err != nil {
		t.Fatal(err)
	}

	cli2 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli2.Connect(ctx, ev1.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	defer cli2.Close()

	ev2, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait ev2: %v", err)
	}
	if seg := testutil.RequireIntField(t, ev2, "segment"); seg != 2 {
		t.Fatalf("resume segment = %d want 2", seg)
	}
	if off := testutil.RequireIntField(t, ev2, "offset"); off != int64(len(`{"seq":2,"msg":"seg2-a"}`)+1) {
		t.Fatalf("resume first offset = %d", off)
	}
	ev3, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait ev3: %v", err)
	}
	if rec, _ := ev3.Field("record"); rec.(map[string]any)["seq"] != float64(3) {
		t.Fatalf("ev3 = %s", ev3.Data)
	}
	// Idempotent: reconnecting again with the same id replays the same event.
	cli3 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli3.Connect(ctx, ev1.ID); err != nil {
		t.Fatalf("resume2: %v", err)
	}
	defer cli3.Close()
	again, err := testutil.WaitForEvent(cli3.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("wait again: %v", err)
	}
	if again.ID != ev2.ID {
		t.Fatalf("replay mismatch: %s vs %s", again.ID, ev2.ID)
	}
}
