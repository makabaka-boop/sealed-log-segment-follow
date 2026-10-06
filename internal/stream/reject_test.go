package stream_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"logfollow/internal/logdir"
	"logfollow/internal/testutil"
)

func resumeExpect(t *testing.T, url, token string, wantStatus int, wantReason string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+"/stream?cursor="+token, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d body=%s, want %d", resp.StatusCode, body, wantStatus)
	}
	if wantReason != "" && !strings.Contains(string(body), wantReason) {
		t.Fatalf("body %s does not contain reason %q", body, wantReason)
	}
}

func restoreSegment(dir string, n int, content []byte) error {
	return os.WriteFile(logdir.SegmentPath(dir, n), content, 0o644)
}

// TestResumeRejections covers every explicit "cannot resume" branch:
// malformed token, run mismatch, deleted cursor segment, segment gap,
// non-newline boundary and out-of-range offset.
func TestResumeRejections(t *testing.T) {
	p, dir := newProducer(t, "run-X")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ctx

	// Segment 1: two lines; then roll to segment 2.
	line1 := []byte("{\"seq\":1}\n")  // 10 bytes: {"seq":1}\n
	line2 := []byte("{\"seq\":22}\n") // 11 bytes
	if err := p.Append(line1); err != nil {
		t.Fatal(err)
	}
	if err := p.Append(line2); err != nil {
		t.Fatal(err)
	}
	boundaryAfterLine1 := logdir.EncodeCursor(logdir.Cursor{RunID: "run-X", Segment: 1, Offset: int64(len(line1))})
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}

	// 1. Garbage token -> 400 malformed.
	resumeExpect(t, srv.URL, "not-a-cursor", http.StatusBadRequest, "malformed_cursor")

	// 2. Run id mismatch -> 409 run_mismatch.
	resumeExpect(t, srv.URL,
		logdir.EncodeCursor(logdir.Cursor{RunID: "run-OTHER", Segment: 1, Offset: int64(len(line1))}),
		http.StatusConflict, "run_mismatch")

	// 3. Offset one byte before a real newline boundary -> 409. The byte is
	//    read from disk; rune-count arithmetic can never satisfy this.
	resumeExpect(t, srv.URL,
		logdir.EncodeCursor(logdir.Cursor{RunID: "run-X", Segment: 1, Offset: int64(len(line1)) - 1}),
		http.StatusConflict, "not_line_boundary")

	// 4. Offset beyond file size -> 409 offset_out_of_range.
	resumeExpect(t, srv.URL,
		logdir.EncodeCursor(logdir.Cursor{RunID: "run-X", Segment: 1, Offset: 4096}),
		http.StatusConflict, "offset_out_of_range")

	// 5. Deleted cursor segment while higher segments remain -> 409.
	if err := p.Delete(1); err != nil {
		t.Fatal(err)
	}
	resumeExpect(t, srv.URL, boundaryAfterLine1, http.StatusConflict, "segment_missing")

	// Rebuild a gappy layout 1,3 (2 missing), resume at 1 -> 409 segment_gap.
	if err := restoreSegment(dir, 1, line1); err != nil {
		t.Fatal(err)
	}
	if err := restoreSegment(dir, 2, nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Roll(); err != nil { // active 2 -> 3
		t.Fatal(err)
	}
	if err := p.Delete(2); err != nil {
		t.Fatal(err)
	}
	resumeExpect(t, srv.URL, boundaryAfterLine1, http.StatusConflict, "segment_gap")

	// A fresh connection must surface the gap as a fatal event while tailing,
	// never silently jump from segment 1 to segment 3.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx2, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "fatal"
	})
	if err != nil {
		t.Fatalf("wait fatal: %v", err)
	}
	if !strings.Contains(string(ev.Data), "segment_gap") {
		t.Fatalf("fatal = %s want segment_gap", ev.Data)
	}
	// The single surviving record may arrive before the fatal; it must be
	// segment 1 content only.
	for _, early := range testutil.Drain(cli.Events(), 0) {
		if seg := testutil.RequireIntField(t, early, "segment"); seg != 1 {
			t.Fatalf("record from beyond the gap was delivered: %s", early.Data)
		}
	}
}

// TestRunIDChanged rejects resume and ends a live subscription when the
// producer marker changes; a new run streams under the new id.
func TestRunIDChanged(t *testing.T) {
	p, dir := newProducer(t, "run-OLD")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.AppendJSON(1, "old"); err != nil {
		t.Fatal(err)
	}
	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	ev, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatal(err)
	}

	// Live marker change: the open subscription fails explicitly; old-run
	// resume is refused with 409 instead of jumping to the new run.
	if err := p.SetRunID("run-NEW"); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(2, "new"); err != nil {
		t.Fatal(err)
	}

	fatal, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "fatal"
	})
	if err != nil {
		t.Fatalf("wait fatal: %v", err)
	}
	if !strings.Contains(string(fatal.Data), "run_changed") {
		t.Fatalf("fatal = %s want run_changed", fatal.Data)
	}

	cli2 := testutil.NewSSEClient(srv.URL, 0)
	err = cli2.Connect(ctx, ev.ID)
	if he, ok := err.(*testutil.HTTPError); !ok || he.Status != http.StatusConflict {
		cli2.Close()
		t.Fatalf("old-run resume = %v, want 409", err)
	}

	// Fresh connection streams new-run bytes tagged with the new run id.
	cli3 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli3.Connect(ctx, ""); err != nil {
		t.Fatalf("connect3: %v", err)
	}
	defer cli3.Close()
	ev3, err := testutil.WaitForEvent(cli3.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		if e.Event != "record" {
			return false
		}
		run, _ := e.Field("run")
		return run == "run-NEW"
	})
	if err != nil {
		t.Fatalf("wait new run: %v", err)
	}
	rec, _ := ev3.Field("record")
	if rec.(map[string]any)["seq"] != float64(1) {
		t.Fatalf("new run record = %s", ev3.Data)
	}
}
