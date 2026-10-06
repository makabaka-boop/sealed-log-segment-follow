package follower

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testTimeout = 3 * time.Second

type testLogDir string

func (d testLogDir) path(seq int) string {
	return filepath.Join(string(d), fmt.Sprintf("segment-%04d.ndjson", seq))
}

func (d testLogDir) writeRaw(seq int, data []byte) {
	t := testingT()
	if err := os.WriteFile(d.path(seq), data, 0o644); err != nil {
		t.Fatalf("write segment %d: %v", seq, err)
	}
}

func (d testLogDir) appendRaw(seq int, data []byte) {
	t := testingT()
	f, err := os.OpenFile(d.path(seq), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open append segment %d: %v", seq, err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatalf("append segment %d: %v", seq, err)
	}
}

func (d testLogDir) appendRecord(seq int, v any) int64 {
	t := testingT()
	st, err := os.Stat(d.path(seq))
	var before int64
	if err == nil {
		before = st.Size()
	}
	line, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	line = append(line, '\n')
	d.appendRaw(seq, line)
	return before + int64(len(line))
}

func (d testLogDir) create(seq int) { d.appendRaw(seq, nil) }

func (d testLogDir) remove(seq int) {
	if err := os.Remove(d.path(seq)); err != nil {
		testingT().Fatalf("remove segment %d: %v", seq, err)
	}
}

func (d testLogDir) replace(seq int, data []byte) {
	t := testingT()
	tmp := d.path(seq) + ".new"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, d.path(seq)); err != nil {
		t.Fatal(err)
	}
}

func (d testLogDir) bytes(seq int) []byte {
	b, err := os.ReadFile(d.path(seq))
	if err != nil {
		testingT().Fatalf("read segment %d: %v", seq, err)
	}
	return b
}

var currentTestingT *testing.T

func testingT() *testing.T { return currentTestingT }

func newTestFollower(t *testing.T, runID string, buf int) (*Follower, *Barrier, testLogDir) {
	currentTestingT = t
	dir := testLogDir(t.TempDir())
	barrier := NewBarrier()
	f, err := New(Config{
		Directory:    string(dir),
		RunID:        runID,
		PollInterval: 5 * time.Millisecond,
		EventBuffer:  buf,
		Barrier:      barrier,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { currentTestingT = nil })
	return f, barrier, dir
}

func barrierWait(t *testing.T, b *Barrier) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("read barrier: %v", err)
	}
}

func nextEvent(t *testing.T, sub *Subscription) Event {
	t.Helper()
	select {
	case ev := <-sub.Events():
		return ev
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for event; terminal=%v", sub.Err())
		return Event{}
	}
}

func assertNoEvent(t *testing.T, sub *Subscription, wait time.Duration) {
	t.Helper()
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event: %+v", ev)
	case <-sub.Done():
		t.Fatalf("subscription ended unexpectedly: %v", sub.Err())
	case <-time.After(wait):
	}
}

func waitTerminal(t *testing.T, sub *Subscription) error {
	t.Helper()
	select {
	case <-sub.Done():
		return sub.Err()
	case <-time.After(testTimeout):
		t.Fatal("subscription did not terminate")
		return nil
	}
}

func eventRaw(ev Event) []byte {
	if ev.Result != nil {
		return ev.Result
	}
	return ev.Raw
}

func verifyOffset(t *testing.T, dir testLogDir, ev Event, raw []byte) {
	t.Helper()
	data := dir.bytes(ev.Segment)
	if ev.EndOffset > int64(len(data)) {
		t.Fatalf("end_offset %d beyond file size %d", ev.EndOffset, len(data))
	}
	if ev.EndOffset < 1 || data[ev.EndOffset-1] != '\n' {
		t.Fatalf("end_offset %d is not a newline boundary", ev.EndOffset)
	}
	// Find the start of this line and compare bytes (not runes).
	start := int64(0)
	if i := bytes.LastIndexByte(data[:ev.EndOffset-1], '\n'); i >= 0 {
		start = int64(i) + 1
	}
	got := data[start : ev.EndOffset-1]
	got = bytes.TrimSuffix(got, []byte{'\r'})
	if !bytes.Equal(got, raw) {
		t.Fatalf("record at segment %d offset %d = %q, want %q", ev.Segment, ev.EndOffset, got, raw)
	}
}

func TestAppendRecordsWithByteOffsets(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	barrierWait(t, barrier) // let one empty poll land

	want1 := dir.appendRecord(1, map[string]any{"n": 1})
	barrierWait(t, barrier)
	ev := nextEvent(t, sub)
	if ev.Type != "record" || ev.RunID != "run-A" || ev.Segment != 1 || ev.EndOffset != want1 {
		t.Fatalf("unexpected event: %+v", ev)
	}
	verifyOffset(t, dir, ev, eventRaw(ev))

	// Half a UTF-8 sequence must stay buffered as bytes, never reported early.
	dir.appendRaw(1, []byte("{\"snowman\":\"x\xe2")) // 0xE2 starts U+2603
	barrierWait(t, barrier)
	assertNoEvent(t, sub, 40*time.Millisecond)

	dir.appendRaw(1, []byte("\x98\x83\"}\n"))
	barrierWait(t, barrier)
	ev = nextEvent(t, sub)
	verifyOffset(t, dir, ev, eventRaw(ev))
	var parsed struct {
		Snowman string `json:"snowman"`
	}
	if err := json.Unmarshal(ev.Result, &parsed); err != nil || parsed.Snowman != "x☃" {
		t.Fatalf("split UTF-8 record not reassembled: %s err=%v", ev.Result, err)
	}
}

func TestResumeFromLastEventIDWithoutDuplication(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	dir.appendRecord(1, map[string]int{"i": 0})
	firstEnd := dir.appendRecord(1, map[string]int{"i": 1})

	ctx1, cancel1 := context.WithCancel(context.Background())
	sub1, err := f.Open(ctx1, nil)
	if err != nil {
		t.Fatal(err)
	}
	nextEvent(t, sub1)
	ev1 := nextEvent(t, sub1)
	if ev1.EndOffset != firstEnd {
		t.Fatalf("offset = %d want %d", ev1.EndOffset, firstEnd)
	}
	cancel1()
	if err := waitTerminal(t, sub1); err != nil {
		// Cancellation is reported only for context-driven disconnects.
		var fe *FollowError
		if !errors.As(err, &fe) || fe.Code != CodeCanceled {
			t.Fatalf("unexpected terminal: %v", err)
		}
	}

	// Simulate a real disconnect/reconnect with the Last-Event-ID value.
	cursor := Cursor{Version: cursorVersion, RunID: "run-A", Segment: ev1.Segment, Offset: ev1.EndOffset}
	thirdEnd := dir.appendRecord(1, map[string]int{"i": 2})

	sub2, err := f.Open(context.Background(), &cursor)
	if err != nil {
		t.Fatalf("resume rejected: %v", err)
	}
	barrierWait(t, barrier)
	ev2 := nextEvent(t, sub2)
	if ev2.Segment != 1 || ev2.EndOffset != thirdEnd {
		t.Fatalf("resumed at wrong position: %+v", ev2)
	}
	var v struct {
		I int `json:"i"`
	}
	if err := json.Unmarshal(ev2.Result, &v); err != nil || v.I != 2 {
		t.Fatalf("got %+v from %s", v, ev2.Result)
	}
}

func TestRotationDoesNotSpliceSegments(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	end1 := dir.appendRecord(1, map[string]string{"k": "in-segment-1"})
	dir.appendRecord(1, map[string]string{"k": "also-segment-1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev := nextEvent(t, sub); ev.EndOffset != end1 {
		t.Fatalf("first event offset %d want %d", ev.EndOffset, end1)
	}
	nextEvent(t, sub)
	barrierWait(t, barrier)

	// Producer seals segment 1 and starts segment 2.
	dir.create(2)
	barrierWait(t, barrier) // observe rotation with no pending bytes
	head2 := dir.appendRecord(2, map[string]string{"k": "in-segment-2"})
	barrierWait(t, barrier)

	ev := nextEvent(t, sub)
	if ev.Segment != 2 || ev.EndOffset != head2 {
		t.Fatalf("first event after rotation = seg %d off %d, want seg 2 off %d", ev.Segment, ev.EndOffset, head2)
	}
}

func TestSealedSegmentMissingNewlineTerminatesSubscription(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	barrierWait(t, barrier)

	// A half line is patiently waited on while the segment is the newest.
	dir.appendRaw(1, []byte(`{"partial":true`))
	barrierWait(t, barrier)
	assertNoEvent(t, sub, 40*time.Millisecond)

	// Rotating without completing the line makes segment 1 sealed-but-truncated.
	dir.create(2)
	dir.appendRecord(2, map[string]string{"k": "new-segment"})
	barrierWait(t, barrier)

	terr := waitTerminal(t, sub)
	var fe *FollowError
	if !errors.As(terr, &fe) || fe.Code != CodeSealedTruncated {
		t.Fatalf("terminal = %v, want sealed truncation", terr)
	}
	if fe.Segment != 1 {
		t.Fatalf("truncation reported on segment %d want 1", fe.Segment)
	}
	// The head of segment 2 must not have been delivered or spliced in.
	select {
	case ev := <-sub.Events():
		t.Fatalf("segment 2 record leaked after truncation: %+v", ev)
	default:
	}

	// A fresh client must also be refused rather than jumping to segment 2.
	if _, err := f.Open(context.Background(), nil); err != nil {
		var fe2 *FollowError
		if !errors.As(err, &fe2) || fe2.Code != CodeSealedTruncated {
			t.Fatalf("fresh open error = %v, want sealed truncation", err)
		}
	} else {
		t.Fatal("fresh open unexpectedly succeeded against truncated history")
	}
}

func TestInvalidJSONLineIsDeliveredAndDoesNotStopStream(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	barrierWait(t, barrier)

	raw := []byte("{not json")
	dir.appendRaw(1, append(append([]byte(nil), raw...), '\n'))
	goodEnd := dir.appendRecord(1, map[string]bool{"ok": true})
	barrierWait(t, barrier)

	bad := nextEvent(t, sub)
	if bad.Type != "invalid_record" || bad.Error == nil || bad.Error.Code != "invalid_json" || !bytes.Equal(bad.Raw, raw) {
		t.Fatalf("bad event = %+v", bad)
	}
	verifyOffset(t, dir, bad, raw)

	good := nextEvent(t, sub)
	if good.Type != "record" || good.EndOffset != goodEnd {
		t.Fatalf("following valid record not delivered: %+v", good)
	}
	verifyOffset(t, dir, good, eventRaw(good))
}

func TestResumeEvidenceGaps(t *testing.T) {
	cases := []struct {
		name string
		prep func(dir testLogDir)
		c    Cursor
		code string
	}{
		{
			name: "run id changed",
			prep: func(dir testLogDir) { dir.appendRecord(1, 1) },
			c:    Cursor{Version: cursorVersion, RunID: "run-OTHER", Segment: 1, Offset: 0},
			code: CodeRunChanged,
		},
		{
			name: "cursor segment deleted",
			prep: func(dir testLogDir) {
				dir.appendRecord(1, 1)
				dir.appendRecord(3, 3)
				dir.remove(1)
			},
			c:    Cursor{Version: cursorVersion, RunID: "run-A", Segment: 1, Offset: 0},
			code: CodeSegmentDeleted,
		},
		{
			name: "sequence gap after cursor",
			prep: func(dir testLogDir) {
				dir.appendRecord(1, 1)
				dir.appendRecord(3, 3)
			},
			c:    Cursor{Version: cursorVersion, RunID: "run-A", Segment: 1, Offset: 0},
			code: CodeSegmentGap,
		},
		{
			name: "offset is not a newline boundary",
			prep: func(dir testLogDir) { dir.appendRecord(1, 1) },
			c:    Cursor{Version: cursorVersion, RunID: "run-A", Segment: 1, Offset: 1},
			code: CodeOffsetBoundary,
		},
		{
			name: "offset beyond eof",
			prep: func(dir testLogDir) { dir.appendRecord(1, 1) },
			c:    Cursor{Version: cursorVersion, RunID: "run-A", Segment: 1, Offset: 1 << 20},
			code: CodeOffsetBeyondEOF,
		},
		{
			name: "fresh start missing segment one",
			prep: func(dir testLogDir) { dir.appendRecord(2, 2) },
			c:    Cursor{}, // unused; signals fresh start below
			code: CodeSegmentGap,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _, dir := newTestFollower(t, "run-A", 4)
			tc.prep(dir)
			var err error
			if tc.code == CodeSegmentGap && tc.c.RunID == "" {
				_, err = f.Open(context.Background(), nil)
			} else {
				c := tc.c
				_, err = f.Open(context.Background(), &c)
			}
			var fe *FollowError
			if !errors.As(err, &fe) || fe.Code != tc.code {
				t.Fatalf("error = %v, want code %s", err, tc.code)
			}
		})
	}
}

func TestCurrentSegmentDeletedDuringFollow(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	dir.appendRecord(1, map[string]int{"i": 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	nextEvent(t, sub)
	barrierWait(t, barrier)

	// Seal 1, write 2, follow into it, then remove the active segment.
	dir.create(2)
	dir.appendRecord(2, map[string]int{"i": 2})
	barrierWait(t, barrier)
	ev := nextEvent(t, sub)
	if ev.Segment != 2 {
		t.Fatalf("expected segment 2 event, got %+v", ev)
	}

	// Ring rotation deletes history: segment 3 appears and segment 2 vanishes.
	dir.create(3)
	dir.remove(2)
	barrierWait(t, barrier)

	terr := waitTerminal(t, sub)
	var fe *FollowError
	if !errors.As(terr, &fe) || fe.Code != CodeSegmentDeleted {
		t.Fatalf("terminal = %v, want cursor_segment_deleted", terr)
	}
}

func TestFileReplacementDetectedByInode(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := f.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir.appendRecord(1, map[string]int{"i": 1})
	barrierWait(t, barrier)
	if ev := nextEvent(t, sub); ev.Segment != 1 {
		t.Fatalf("unexpected event %+v", ev)
	}

	// Same filename, different inode: copy/replace style rotation the service
	// does not support.
	dir.replace(1, []byte("{\"i\":99}\n"))
	barrierWait(t, barrier)

	terr := waitTerminal(t, sub)
	var fe *FollowError
	if !errors.As(terr, &fe) || fe.Code != CodeSegmentReplaced {
		t.Fatalf("terminal = %v, want segment_replaced", terr)
	}

	// A new client reads the replacement as new evidence (fresh identity).
	sub2, err := f.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ev := nextEvent(t, sub2)
	if ev.Segment != 1 {
		t.Fatalf("new client could not follow replaced file: %+v err=%v", ev, sub2.Err())
	}
}

func TestSlowConsumerIsInterruptedAlone(t *testing.T) {
	dir := testLogDir(t.TempDir())
	barrier := NewBarrier()
	mkFollower := func(buf int) *Follower {
		f, err := New(Config{
			Directory:    string(dir),
			RunID:        "run-A",
			PollInterval: 5 * time.Millisecond,
			EventBuffer:  buf,
			Barrier:      barrier,
		})
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow, err := mkFollower(2).Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := mkFollower(16).Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	barrierWait(t, barrier)

	for i := 0; i < 6; i++ {
		dir.appendRecord(1, map[string]int{"i": i})
	}
	barrierWait(t, barrier)

	terr := waitTerminal(t, slow)
	var fe *FollowError
	if !errors.As(terr, &fe) || fe.Code != CodeSlowConsumer {
		t.Fatalf("slow terminal = %v, want slow_consumer", terr)
	}

	// The other subscription must keep receiving every record.
	for i := 0; i < 6; i++ {
		ev := nextEvent(t, fast)
		var v struct {
			I int `json:"i"`
		}
		if err := json.Unmarshal(ev.Result, &v); err != nil || v.I != i {
			t.Fatalf("fast subscriber event %d = %+v (%s)", i, v, ev.Result)
		}
	}
}

func TestSegmentLimitsEnforced(t *testing.T) {
	t.Run("more than six segments", func(t *testing.T) {
		f, _, dir := newTestFollower(t, "run-A", 4)
		for seq := 1; seq <= MaxSegments+1; seq++ {
			dir.appendRecord(seq, seq)
		}
		_, err := f.Open(context.Background(), nil)
		var fe *FollowError
		if !errors.As(err, &fe) || fe.Code != CodeTooManySegments {
			t.Fatalf("error = %v, want too_many_segments", err)
		}
	})

	t.Run("segment larger than 8 KiB", func(t *testing.T) {
		f, _, dir := newTestFollower(t, "run-A", 4)
		dir.writeRaw(1, append(bytes.Repeat([]byte("a"), MaxSegmentSize), '\n'))
		_, err := f.Open(context.Background(), nil)
		var fe *FollowError
		if !errors.As(err, &fe) || fe.Code != CodeSegmentOversized {
			t.Fatalf("error = %v, want segment_too_large", err)
		}
	})

	t.Run("exactly 8 KiB is allowed", func(t *testing.T) {
		f, _, dir := newTestFollower(t, "run-A", 4)
		line := append(bytes.Repeat([]byte("x"), MaxSegmentSize-1), '\n')
		dir.writeRaw(1, line)
		sub, err := f.Open(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ev := nextEvent(t, sub)
		if ev.EndOffset != MaxSegmentSize {
			t.Fatalf("end offset = %d want %d", ev.EndOffset, MaxSegmentSize)
		}
	})
}

func TestCursorEncodingRoundTrip(t *testing.T) {
	id := EncodeCursor("run-A", 3, 42)
	c, err := DecodeCursor(id)
	if err != nil {
		t.Fatal(err)
	}
	if c.RunID != "run-A" || c.Segment != 3 || c.Offset != 42 {
		t.Fatalf("round trip mismatch: %+v", c)
	}
	for _, bad := range []string{"not-base64!!", "eyJ2IjoxfQ", "  ", "{}"} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Fatalf("accepted bad cursor %q", bad)
		}
	}
}

// ---- HTTP/SSE integration with real reconnects ----

type sseMessage struct {
	event string
	id    string
	data  string
}

func readSSE(t *testing.T, r *bufio.Reader) sseMessage {
	t.Helper()
	var msg sseMessage
	var dataLines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			msg.data = strings.Join(dataLines, "\n")
			return msg
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("malformed SSE line %q", line)
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			msg.event = value
		case "id":
			msg.id = value
		case "data":
			dataLines = append(dataLines, value)
		case "", "retry", "comment":
		}
	}
}

func TestHTTPStreamResumeAndReconnect(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 16)
	mux := http.NewServeMux()
	srv := &Server{Follow: f, Barrier: barrier}
	srv.Routes(mux)
	ts := http.NewServeMux()
	ts.Handle("/", mux)
	httpServer := newHTTPTestServer(t, ts)

	get := func(lastID string) (*http.Response, *bufio.Reader) {
		req, _ := http.NewRequest(http.MethodGet, httpServer.URL+"/logs", nil)
		if lastID != "" {
			req.Header.Set("Last-Event-ID", lastID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
			t.Fatalf("content type = %q", ct)
		}
		if resp.Header.Get("X-Log-Run-ID") != "run-A" {
			t.Fatalf("run header = %q", resp.Header.Get("X-Log-Run-ID"))
		}
		return resp, bufio.NewReader(resp.Body)
	}

	resp1, reader1 := get("")
	dir.appendRecord(1, map[string]int{"i": 1})
	syncBarrierHTTP(t, httpServer.URL)
	msg1 := readSSE(t, reader1)
	if msg1.event != "record" || msg1.id == "" {
		t.Fatalf("bad SSE frame: %+v", msg1)
	}
	var ev1 Event
	if err := json.Unmarshal([]byte(msg1.data), &ev1); err != nil {
		t.Fatal(err)
	}
	if ev1.RunID != "run-A" || ev1.Segment != 1 {
		t.Fatalf("bad event payload: %s", msg1.data)
	}
	lastID := msg1.id
	resp1.Body.Close() // disconnect

	dir.appendRecord(1, map[string]int{"i": 2})
	syncBarrierHTTP(t, httpServer.URL)

	resp2, reader2 := get(lastID) // reconnect using browser-style header
	defer resp2.Body.Close()
	msg2 := readSSE(t, reader2)
	var ev2 Event
	if err := json.Unmarshal([]byte(msg2.data), &ev2); err != nil || ev2.EndOffset <= ev1.EndOffset {
		t.Fatalf("reconnect delivered wrong event: %+v (%s)", ev2, msg2.data)
	}

	// A mid-stream truncation surfaces as an SSE error event with a reason.
	dir.appendRaw(1, []byte(`{"dangling"`))
	dir.create(2)
	syncBarrierHTTP(t, httpServer.URL)
	msgErr := readSSE(t, reader2)
	if msgErr.event != "error" {
		t.Fatalf("expected terminal error event, got %+v", msgErr)
	}
	var term TerminalEvent
	if err := json.Unmarshal([]byte(msgErr.data), &term); err != nil {
		t.Fatal(err)
	}
	if term.Error == nil || term.Error.Code != CodeSealedTruncated {
		t.Fatalf("terminal payload = %s", msgErr.data)
	}
}

func TestHTTPRejectsStaleRun(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 4)
	dir.appendRecord(1, 1)
	mux := http.NewServeMux()
	(&Server{Follow: f, Barrier: barrier}).Routes(mux)
	httpServer := newHTTPTestServer(t, mux)

	cursor := EncodeCursor("run-OLD", 1, 0)
	req, _ := http.NewRequest(http.MethodGet, httpServer.URL+"/logs", nil)
	req.Header.Set("Last-Event-ID", cursor)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d want 409", resp.StatusCode)
	}
	var body struct {
		Error *FollowError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error == nil || body.Error.Code != CodeRunChanged {
		t.Fatalf("body = %+v", body)
	}
}

func TestInvalidUTF8CompleteLineReported(t *testing.T) {
	f, barrier, dir := newTestFollower(t, "run-A", 4)
	sub, err := f.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("{\"bad\":\"\xff\xfe\"}")
	dir.appendRaw(1, append(append([]byte(nil), raw...), '\n'))
	barrierWait(t, barrier)
	ev := nextEvent(t, sub)
	if ev.Type != "invalid_record" || !bytes.Equal(ev.Raw, raw) {
		t.Fatalf("event = %+v", ev)
	}
	verifyOffset(t, dir, ev, raw)

	// The exact offending bytes must survive JSON transport of the event too.
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Event
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Raw, raw) {
		t.Fatalf("raw bytes changed through JSON transport: %v", decoded.Raw)
	}
}
