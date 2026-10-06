// Package stream exposes the follower over Server-Sent Events with
// Last-Event-ID resumption, per-connection bounded buffers and independent
// cancellation of slow clients.
package stream

import (
	"encoding/json"
	"net/http"
	"time"

	"logfollow/internal/follow"
	"logfollow/internal/logdir"
)

// Config tunes the HTTP/SSE layer.
type Config struct {
	Dir          string
	PollInterval time.Duration
	Buffer       int
	FullWait     time.Duration
	Heartbeat    time.Duration
	FatalGrace   time.Duration
}

func (cfg *Config) normalize() {
	if cfg.Buffer <= 0 {
		cfg.Buffer = 256
	}
	if cfg.FullWait <= 0 {
		cfg.FullWait = 2 * time.Second
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.FatalGrace <= 0 {
		cfg.FatalGrace = 5 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Millisecond
	}
}

// Server holds the immutable stream configuration.
type Server struct {
	cfg Config
}

// NewServer builds an SSE server.
func NewServer(cfg Config) *Server {
	cfg.normalize()
	return &Server{cfg: cfg}
}

// Mux registers the HTTP routes.
func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", s.handleStream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

type errorPayload struct {
	Error  string         `json:"error"`
	Reason string         `json:"reason"`
	Cursor *logdir.Cursor `json:"cursor,omitempty"`
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var start logdir.Cursor
	resuming := false
	token := r.Header.Get("Last-Event-ID")
	if q := r.URL.Query().Get("cursor"); q != "" {
		token = q
	}
	if token != "" {
		c, err := logdir.DecodeCursor(token)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, errorPayload{
				Error:  err.Error(),
				Reason: follow.ResumeMalformed,
			})
			return
		}
		if re := follow.ResumeCheck(s.cfg.Dir, c); re != nil {
			// Never silently move an invalid cursor to the current tail:
			// 409 with an explicit, machine-readable reason.
			writeJSONError(w, http.StatusConflict, errorPayload{
				Error:  re.Message,
				Reason: re.Reason,
				Cursor: &re.Cursor,
			})
			return
		}
		start = c
		resuming = true
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush() // let the client start parsing before the first event
	}

	sink, fatalCh := follow.Run(ctx, follow.Config{
		Dir:          s.cfg.Dir,
		PollInterval: s.cfg.PollInterval,
		Buffer:       s.cfg.Buffer,
		FullWait:     s.cfg.FullWait,
	}, start, resuming)
	// The follower goroutine owns sink.Close(); the handler must not close a
	// channel the producer may still send on.

	s.streamLoop(w, ctx.Done(), sink.Events(), fatalCh)
}

func (s *Server) streamLoop(
	w http.ResponseWriter,
	connDone <-chan struct{},
	events <-chan logdir.Event,
	fatalCh <-chan *logdir.FatalError,
) {
	heartbeat := time.NewTicker(s.cfg.Heartbeat)
	defer heartbeat.Stop()
	flusher, _ := w.(http.Flusher)

	ended := false
	var fatal *logdir.FatalError
	for !ended {
		// Events that reached the bounded queue before the terminal
		// condition are always delivered first and in order.
		if events != nil {
			if ev, got, more := tryRecv(events); !more {
				// Closed: the follower may still be about to publish its
				// fatal reason (it sends fatal around the same moment). Nil
				// the channel and keep selecting so the fatal is never lost.
				events = nil
			} else if got {
				if !writeEventFrame(w, flusher, ev) {
					return
				}
				continue
			}
		}
		select {
		case <-connDone:
			return
		case fe, ok := <-fatalCh:
			if ok {
				fatal = fe
			}
			ended = true
		case ev, ok := <-events:
			if !ok {
				events = nil
				break
			}
			if !writeEventFrame(w, flusher, ev) {
				return
			}
		case <-heartbeat.C:
			// Heartbeat gets a bounded deadline: an idle dead peer must not
			// pin the goroutine forever. Record frames carry no deadline, so
			// a momentarily slow client keeps its full grace window.
			if !writeDeadlineFrame(w, []byte(": keep-alive\n\n"), s.cfg.FatalGrace) {
				return
			}
			flushIf(flusher)
		}
	}

	if fatal == nil {
		return // follower ended cleanly (request context cancelled)
	}

	// Drain everything that entered the queue before the fatal so the
	// terminal frame is always the final frame. Each drain write carries the
	// grace deadline: a peer that stays blocked must not prevent delivery of
	// the terminal reason.
	for {
		ev, got, more := tryRecv(events)
		if !more || !got {
			break
		}
		if !writeEventFrameDeadline(w, flusher, ev, s.cfg.FatalGrace) {
			return
		}
	}

	// The writer may be blocked in the kernel on a peer that stopped reading;
	// bound delivery so the goroutine cannot leak. First attempt gets a grace
	// deadline; if an in-flight write already consumed it, wait once and
	// retry with a short hard deadline.
	if !writeDeadlineFrame(w, fatalFrame(fatal), s.cfg.FatalGrace) {
		t := time.NewTimer(s.cfg.FatalGrace)
		select {
		case <-connDone:
			t.Stop()
			return
		case <-t.C:
		}
		if !writeDeadlineFrame(w, fatalFrame(fatal), time.Second) {
			return
		}
	}
	flushIf(flusher)
}

// tryRecv performs one non-blocking receive. got is false when no value was
// ready; more is false when the channel is closed.
func tryRecv(ch <-chan logdir.Event) (ev logdir.Event, got, more bool) {
	select {
	case ev, ok := <-ch:
		if !ok {
			return logdir.Event{}, false, false
		}
		return ev, true, true
	default:
		return logdir.Event{}, false, true
	}
}

func writeEventFrame(w http.ResponseWriter, flusher http.Flusher, ev logdir.Event) bool {
	return writeEventFrameDeadline(w, flusher, ev, 0)
}

func writeEventFrameDeadline(w http.ResponseWriter, flusher http.Flusher, ev logdir.Event, deadline time.Duration) bool {
	id := logdir.EncodeCursor(ev.Cursor())
	payload := buildPayload(ev)
	var frame []byte
	frame = append(frame, []byte("id: "+id+"\n")...)
	frame = append(frame, []byte("event: "+ev.Kind+"\n")...)
	frame = append(frame, []byte("data: ")...)
	frame = append(frame, payload...)
	frame = append(frame, '\n', '\n')
	if !writeDeadlineFrame(w, frame, deadline) {
		return false
	}
	flushIf(flusher)
	return true
}

func buildPayload(ev logdir.Event) []byte {
	type envelope struct {
		Run     string          `json:"run"`
		Segment int             `json:"segment"`
		Offset  int64           `json:"offset"`
		Record  json.RawMessage `json:"record,omitempty"`
		Line    string          `json:"line,omitempty"`
		Error   string          `json:"error,omitempty"`
	}
	env := envelope{Run: ev.RunID, Segment: ev.Segment, Offset: ev.Offset}
	switch ev.Kind {
	case logdir.KindRecord:
		env.Record = ev.Record
	case logdir.KindBadJSON:
		env.Line = string(ev.Line)
		env.Error = ev.Error
	}
	raw, err := json.Marshal(env)
	if err != nil {
		raw = []byte(`{"error":"internal marshal failure"}`)
	}
	return raw
}

func fatalFrame(fe *logdir.FatalError) []byte {
	type fatalEnvelope struct {
		Kind   string        `json:"kind"`
		Error  string        `json:"error"`
		Cursor logdir.Cursor `json:"cursor"`
	}
	raw, err := json.Marshal(fatalEnvelope{
		Kind:   fe.Kind,
		Error:  fe.Message,
		Cursor: fe.Cursor,
	})
	if err != nil {
		raw = []byte(`{"kind":"internal_error","error":"internal marshal failure"}`)
	}
	var frame []byte
	frame = append(frame, []byte("event: fatal\n")...)
	frame = append(frame, []byte("data: ")...)
	frame = append(frame, raw...)
	frame = append(frame, '\n', '\n')
	return frame
}

func writeDeadlineFrame(w http.ResponseWriter, frame []byte, deadline time.Duration) bool {
	if deadline > 0 {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(deadline))
	}
	if _, err := w.Write(frame); err != nil {
		return false
	}
	if deadline > 0 {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	}
	return true
}

func flushIf(f http.Flusher) {
	if f != nil {
		f.Flush()
	}
}

func writeJSONError(w http.ResponseWriter, status int, p errorPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}
