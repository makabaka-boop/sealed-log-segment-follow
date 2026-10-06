package testutil

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SSEEvent is one parsed Server-Sent Events dispatch.
type SSEEvent struct {
	ID    string
	Event string
	Data  json.RawMessage
}

// Field decodes the data object into a generic map.
func (e SSEEvent) Field(key string) (any, bool) {
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, false
	}
	v, ok := m[key]
	return v, ok
}

// MustField returns the field or fails through the supplied logger.
func (e SSEEvent) MustField(tb TB, key string) any {
	tb.Helper()
	v, ok := e.Field(key)
	if !ok {
		tb.Fatalf("event data missing field %q: %s", key, e.Data)
	}
	return v
}

// TB is the subset of testing.T used by helpers.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// SSEClient is a streaming test client with an independently settable socket
// receive buffer (needed to reproduce slow consumers deterministically).
type SSEClient struct {
	baseURL  string
	rcvBuf   int
	parseBuf int
	httpCli  *http.Client
	cancel   context.CancelFunc
	resp     *http.Response
	conn     net.Conn // raw TCP conn captured during dial
	reader   *bufio.Reader
	evCh     chan SSEEvent
	errCh    chan error
}

// NewSSEClient builds a client. rcvBuf<=0 leaves the socket default.
func NewSSEClient(baseURL string, rcvBuf int) *SSEClient {
	return NewSSEClientSized(baseURL, rcvBuf, 0)
}

// NewSSEClientSized also sets the initial read buffer; a small value limits
// how much a "non-reading" test client can absorb before back-pressuring the
// server. The reader grows for long lines rather than erroring.
func NewSSEClientSized(baseURL string, rcvBuf, parseBuf int) *SSEClient {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	c := &SSEClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		rcvBuf:   rcvBuf,
		parseBuf: parseBuf,
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil || rcvBuf <= 0 {
				return conn, err
			}
			// SO_RCVBUF bounds the window the peer can fill unread, which is
			// what blocks the server writer for a non-reading client.
			if rc, ok := conn.(*net.TCPConn); ok {
				_ = rc.SetReadBuffer(rcvBuf)
			}
			c.conn = conn
			return conn, nil
		},
	}
	c.httpCli = &http.Client{Transport: tr}
	return c
}

// Connect opens GET /stream, optionally with a Last-Event-ID.
func (c *SSEClient) Connect(ctx context.Context, lastEventID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/stream", nil)
	if err != nil {
		return err
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := c.httpCli.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return &HTTPError{Status: resp.StatusCode, Body: body}
	}
	subCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.resp = resp
	readSize := 64 * 1024
	if c.parseBuf > 0 {
		readSize = c.parseBuf
	}
	c.reader = bufio.NewReaderSize(resp.Body, readSize)
	c.evCh = make(chan SSEEvent, 256)
	c.errCh = make(chan error, 1)
	go c.parse(subCtx)
	return nil
}

// HTTPError carries a non-200 stream response.
type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, strings.TrimSpace(string(e.Body)))
}

func (c *SSEClient) parse(ctx context.Context) {
	defer close(c.evCh)
	var id, event string
	var dataLines [][]byte
	hasField := false

	dispatch := func() {
		if !hasField {
			return
		}
		ev := SSEEvent{ID: id, Event: event}
		if len(dataLines) > 0 {
			ev.Data = json.RawMessage(bytes.Join(dataLines, []byte{'\n'}))
		}
		select {
		case c.evCh <- ev:
		case <-ctx.Done():
		}
		id, event, dataLines, hasField = "", "", nil, false
	}

	for {
		raw, err := c.reader.ReadString('\n')
		// SSE lines end in \n; a final chunk without one on EOF is ignored
		// (an incomplete dispatch), matching the wire framing.
		line := strings.TrimSuffix(raw, "\n")
		line = strings.TrimSuffix(line, "\r")
		if line == "" && raw != "" {
			dispatch()
		} else if line != "" {
			if strings.HasPrefix(line, ":") {
				// comment / heartbeat
			} else {
				hasField = true
				field, value, found := strings.Cut(line, ":")
				if found && strings.HasPrefix(value, " ") {
					value = value[1:]
				}
				switch field {
				case "id":
					id = value
				case "event":
					event = value
				case "data":
					dataLines = append(dataLines, []byte(value))
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				select {
				case c.errCh <- err:
				default:
				}
			}
			return
		}
	}
}

// Events returns the event channel (closed when the stream ends).
func (c *SSEClient) Events() <-chan SSEEvent { return c.evCh }

// Err returns a transport-level parse error, if any.
func (c *SSEClient) Err() error {
	select {
	case err := <-c.errCh:
		return err
	default:
		return nil
	}
}

// Close terminates the subscription.
func (c *SSEClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.resp != nil {
		_ = c.resp.Body.Close()
	}
}

// FreezeRead stops further reads from the socket without fully closing the
// connection, modelling a dead/slow consumer: with a tiny reader buffer the
// first oversized SSE line exhausts userspace buffering, subsequent bytes stay
// in the kernel receive buffer, the peer's window closes and the server writer
// blocks. The read side is shut (RST-free for the bounded burst) while write
// stays open, so the server does not immediately see EOF.
func (c *SSEClient) FreezeRead() {
	if tc, ok := c.conn.(*net.TCPConn); ok {
		_ = tc.CloseRead()
	}
}

// WaitForBarrier consumes events until it sees record {"barrier":"<tag>"}
// and returns that event. Fatal events make it fail.
func WaitForBarrier(tb TB, ch <-chan SSEEvent, tag string, timeout time.Duration) SSEEvent {
	return waitBarrier(tb, ch, tag, 0, timeout)
}

// WaitForBarrierNum matches a record whose numeric "barrier" field equals n.
func WaitForBarrierNum(tb TB, ch <-chan SSEEvent, n int, timeout time.Duration) SSEEvent {
	return waitBarrier(tb, ch, "", n, timeout)
}

func waitBarrier(tb TB, ch <-chan SSEEvent, tag string, n int, timeout time.Duration) SSEEvent {
	tb.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				tb.Fatalf("stream closed before barrier %q/%d", tag, n)
			}
			if ev.Event == "fatal" {
				tb.Fatalf("got fatal event while waiting for barrier %q/%d: %s", tag, n, ev.Data)
			}
			if ev.Event != "record" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(ev.Data, &m); err != nil {
				continue
			}
			rec, ok := m["record"].(map[string]any)
			if !ok {
				continue
			}
			if n != 0 {
				if num, ok := rec["barrier"].(float64); ok && int(num) == n {
					return ev
				}
			} else if rec["barrier"] == tag {
				return ev
			}
		case <-deadline:
			tb.Fatalf("timed out waiting for barrier %q/%d", tag, n)
		}
	}
}

// WaitForEvent waits for the first event matching pred (or any event when
// pred is nil) and returns it.
func WaitForEvent(ch <-chan SSEEvent, timeout time.Duration, pred func(SSEEvent) bool) (SSEEvent, error) {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return SSEEvent{}, io.EOF
			}
			if pred == nil || pred(ev) {
				return ev, nil
			}
		case <-deadline:
			return SSEEvent{}, errors.New("timeout waiting for event")
		}
	}
}

// Drain briefly collects pending events to assert quiet (e.g. a half line is
// not delivered before its newline).
func Drain(ch <-chan SSEEvent, quietFor time.Duration) []SSEEvent {
	var got []SSEEvent
	timer := time.NewTimer(quietFor)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, ev)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(quietFor)
		case <-timer.C:
			return got
		}
	}
}

// RequireIntField reads an integer field from an event's data envelope.
func RequireIntField(tb TB, ev SSEEvent, key string) int64 {
	tb.Helper()
	v := ev.MustField(tb, key)
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			tb.Fatalf("field %q not numeric: %v", key, v)
		}
		return i
	default:
		tb.Fatalf("field %q not numeric: %v", key, v)
		return 0
	}
}
