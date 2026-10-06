package follower

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const sseTermWriteTimeout = 5 * time.Second

// Server exposes the follower HTTP/SSE API.
type Server struct {
	Follow  *Follower
	Barrier *Barrier
}

type apiError struct {
	Error *FollowError `json:"error"`
}

func (s *Server) writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Log-Run-ID", s.Follow.RunID())
	w.WriteHeader(status)
	fe, ok := err.(*FollowError)
	if !ok {
		fe = followError(CodeReadFailed, err.Error(), 0, 0, nil)
	}
	_ = json.NewEncoder(w).Encode(apiError{Error: fe})
}

// Routes installs API routes on mux.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /run-id", s.handleRunID)
	mux.HandleFunc("GET /logs", s.handleLogs)
	mux.HandleFunc("POST /test/read-barrier", s.handleBarrier)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("X-Log-Run-ID", s.Follow.RunID())
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRunID(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Log-Run-ID", s.Follow.RunID())
	_ = json.NewEncoder(w).Encode(map[string]string{"run_id": s.Follow.RunID()})
}

func (s *Server) handleBarrier(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Barrier.Wait(ctx); err != nil {
		s.writeError(w, http.StatusRequestTimeout, followError(CodeReadFailed, "read barrier timed out", 0, 0, err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	var cursor *Cursor
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		c, err := DecodeCursor(id)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err)
			return
		}
		cursor = &c
	}
	if q := r.URL.Query().Get("last-event-id"); q != "" {
		if cursor != nil {
			s.writeError(w, http.StatusBadRequest, followError(CodeInvalidCursor, "provide Last-Event-ID header or query parameter, not both", 0, 0, nil))
			return
		}
		c, err := DecodeCursor(q)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err)
			return
		}
		cursor = &c
	}

	sub, err := s.Follow.Open(r.Context(), cursor)
	if err != nil {
		status := http.StatusInternalServerError
		var fe *FollowError
		if errors.As(err, &fe) {
			switch fe.Code {
			case CodeInvalidCursor, CodeOffsetBoundary:
				status = http.StatusBadRequest
			case CodeRunChanged, CodeSegmentDeleted, CodeSegmentGap,
				CodeOffsetBeyondEOF, CodeSealedTruncated, CodeTooManySegments,
				CodeSegmentOversized:
				status = http.StatusConflict
			}
		}
		s.writeError(w, status, err)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-store")
	h.Set("Connection", "keep-alive")
	h.Set("X-Log-Run-ID", s.Follow.RunID())
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	enc := json.NewEncoder(w)
	var streamErr error
stream:
	for {
		select {
		case <-r.Context().Done():
			streamErr = followError(CodeCanceled, "client disconnected", 0, 0, r.Context().Err())
			break stream
		case <-sub.Done():
			select {
			case streamErr = <-sub.terminal:
			default:
				streamErr = nil
			}
			break stream
		case ev := <-sub.Events():
			id := EncodeCursor(ev.RunID, ev.Segment, ev.EndOffset)
			if _, err := fmt.Fprintf(w, "event: %s\nid: %s\n", ev.Type, id); err != nil {
				streamErr = followError(CodeCanceled, "cannot write SSE event", ev.Segment, ev.EndOffset, err)
				break stream
			}
			if _, err := w.Write([]byte("data: ")); err != nil {
				streamErr = followError(CodeCanceled, "cannot write SSE event", ev.Segment, ev.EndOffset, err)
				break stream
			}
			if err := enc.Encode(ev); err != nil {
				streamErr = followError(CodeCanceled, "cannot encode SSE event", ev.Segment, ev.EndOffset, err)
				break stream
			}
			// json.Encoder.Encode appends a newline; SSE requires an extra blank line.
			if _, err := w.Write([]byte("\n")); err != nil {
				streamErr = followError(CodeCanceled, "cannot write SSE frame", ev.Segment, ev.EndOffset, err)
				break stream
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	var fe *FollowError
	if streamErr != nil && errors.As(streamErr, &fe) && fe.Code != CodeCanceled {
		term := TerminalEvent{
			Type:    "error",
			RunID:   s.Follow.RunID(),
			Segment: fe.Segment,
			Offset:  fe.Offset,
			Error:   &ErrorDetail{Code: fe.Code, Message: fe.Message},
		}
		var body bytes.Buffer
		if err := json.NewEncoder(&body).Encode(term); err == nil {
			if rc, ok := w.(interface{ SetWriteDeadline(time.Time) error }); ok {
				_ = rc.SetWriteDeadline(time.Now().Add(sseTermWriteTimeout))
			}
			_, _ = w.Write([]byte("event: error\n"))
			// Prefix every JSON body line with the SSE data field, then close
			// the frame with a blank line.
			for _, line := range bytes.Split(bytes.TrimRight(body.Bytes(), "\n"), []byte{'\n'}) {
				_, _ = w.Write([]byte("data: "))
				_, _ = w.Write(line)
				_, _ = w.Write([]byte("\n"))
			}
			_, _ = w.Write([]byte("\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}
