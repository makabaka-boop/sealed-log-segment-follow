package stream_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"logfollow/internal/logdir"
	"logfollow/internal/testutil"
)

// TestSixSegmentsWithBarriersAndUTF8 rolls through all six segments (one
// roll per boundary), using read barriers to observe each segment before the
// producer advances, then splits a multibyte UTF-8 record across appends on
// the final segment. Every record's segment number and byte offset are
// checked; one segment's tail can never be glued to the next's head.
func TestSixSegmentsWithBarriersAndUTF8(t *testing.T) {
	p, dir := newProducer(t, "run-six")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	big := fixedLine("fill", 4000) // 4000 bytes
	var barrierOff int64
	for seg := 1; seg <= logdir.MaxSegments; seg++ {
		if err := p.AppendLineWithRoll(big); err != nil {
			t.Fatalf("seg %d fill: %v", seg, err)
		}
		barrierLine := []byte(`{"barrier":` + itoaSeg(seg) + `}` + "\n")
		if err := p.AppendLineWithRoll(barrierLine); err != nil {
			t.Fatalf("seg %d barrier: %v", seg, err)
		}
		// Consume the fill record.
		fill, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
			return e.Event == "record"
		})
		if err != nil {
			t.Fatalf("seg %d fill wait: %v", seg, err)
		}
		if s := testutil.RequireIntField(t, fill, "segment"); int(s) != seg {
			t.Fatalf("seg %d fill arrived on segment %d", seg, s)
		}
		if off := testutil.RequireIntField(t, fill, "offset"); off != 4000 {
			t.Fatalf("seg %d fill offset = %d want 4000", seg, off)
		}
		// Read barrier: the barrier must be observed on THIS segment before
		// we roll further.
		bev := testutil.WaitForBarrierNum(t, cli.Events(), seg, eventTimeout)
		if s := testutil.RequireIntField(t, bev, "segment"); int(s) != seg {
			t.Fatalf("seg %d barrier arrived on segment %d", seg, s)
		}
		barrierOff = testutil.RequireIntField(t, bev, "offset")
		if want := 4000 + int64(len(barrierLine)); barrierOff != want {
			t.Fatalf("seg %d barrier offset = %d want %d", seg, barrierOff, want)
		}
		if seg < logdir.MaxSegments {
			if err := p.Roll(); err != nil {
				t.Fatalf("roll %d->%d: %v", seg, seg+1, err)
			}
		}
	}
	if p.Segment() != 6 {
		t.Fatalf("expected to finish on segment 6, at %d", p.Segment())
	}

	// Split a multibyte UTF-8 record across three appends on segment 6.
	// {"u":"京"}\n = 7-byte prefix + 3-byte rune (E4 BA AC = 京) + 2-byte
	// suffix ("}) + newline = 12 bytes; segment offset 4014 + 12 = 4026.
	if err := p.Append([]byte(`{"u":"`)); err != nil {
		t.Fatal(err)
	}
	if err := p.Append([]byte{0xe4, 0xba}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.Drain(cli.Events(), 100*time.Millisecond); len(got) != 0 {
		t.Fatalf("half multibyte leaked: %+v", got)
	}
	if err := p.Append([]byte{0xac, '"', '}', '\n'}); err != nil {
		t.Fatal(err)
	}
	u, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("utf8 record: %v", err)
	}
	rec, _ := u.Field("record")
	if rec.(map[string]any)["u"] != "京" {
		t.Fatalf("utf8 record = %s", u.Data)
	}
	if off := testutil.RequireIntField(t, u, "offset"); off != 4026 {
		t.Fatalf("utf8 offset = %d want 4026 (4014 + 12)", off)
	}
}

// TestDeletedHistoryAfterProgress: after a subscription has advanced into a
// newer segment, deleting an older sealed segment does not disturb the live
// stream, but a resume token pointing into the deleted history is explicitly
// rejected — the client is never silently relocated to the present.
func TestDeletedHistoryAfterProgress(t *testing.T) {
	p, dir := newProducer(t, "run-del")
	srv := testServer(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seg1Line := []byte(`{"seq":1,"msg":"seg1"}` + "\n")
	if err := p.Append(seg1Line); err != nil {
		t.Fatal(err)
	}
	seg1End := logdir.EncodeCursor(logdir.Cursor{RunID: "run-del", Segment: 1, Offset: int64(len(seg1Line))})
	if err := p.Roll(); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(2, "seg2"); err != nil {
		t.Fatal(err)
	}

	cli := testutil.NewSSEClient(srv.URL, 0)
	if err := cli.Connect(ctx, ""); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	// Fresh client starts at oldest segment: see seg1 then advance to seg2.
	first, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record" && testutil.RequireIntField(testTB(t), e, "segment") == 1
	})
	if err != nil {
		t.Fatalf("seg1 record: %v", err)
	}
	_ = first
	ev2, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record" && testutil.RequireIntField(testTB(t), e, "segment") == 2
	})
	if err != nil {
		t.Fatalf("advance to seg2: %v", err)
	}

	// Delete historical segment 1 while live on segment 2.
	if err := p.Delete(1); err != nil {
		t.Fatal(err)
	}
	if err := p.AppendJSON(3, "still-here"); err != nil {
		t.Fatal(err)
	}
	ev3, err := testutil.WaitForEvent(cli.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("live stream after deleting history: %v", err)
	}
	rec, _ := ev3.Field("record")
	if rec.(map[string]any)["msg"] != "still-here" {
		t.Fatalf("record = %s", ev3.Data)
	}

	// Token into deleted segment 1: 409 segment_missing, never a jump.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream?cursor="+seg1End, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d want 409, body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "segment_missing") {
		t.Fatalf("body = %s, want segment_missing", body)
	}

	// Token on surviving segment 2 resumes normally.
	cli2 := testutil.NewSSEClient(srv.URL, 0)
	if err := cli2.Connect(ctx, ev2.ID); err != nil {
		t.Fatalf("resume on surviving segment 2: %v", err)
	}
	defer cli2.Close()
	next, err := testutil.WaitForEvent(cli2.Events(), eventTimeout, func(e testutil.SSEEvent) bool {
		return e.Event == "record"
	})
	if err != nil {
		t.Fatalf("resume seg2: %v", err)
	}
	r2, _ := next.Field("record")
	if r2.(map[string]any)["msg"] != "still-here" {
		t.Fatalf("resumed record = %s", next.Data)
	}
}

func itoaSeg(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// testTB adapts *testing.T to the helper TB.
func testTB(t *testing.T) testutil.TB { return t }
