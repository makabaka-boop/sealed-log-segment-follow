// Package follow implements the byte-oriented NDJSON follower state machine:
// read-only segment files, byte-offset cursors on newline boundaries,
// sealed/active segment transitions, and detection of name-preserving file
// replacement instead of trusting file names.
package follow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"logfollow/internal/logdir"
)

// Config controls a follower run.
type Config struct {
	// Dir is the log directory.
	Dir string
	// PollInterval is how often the directory is rescanned for new segments,
	// growth, replacement or run-id changes.
	PollInterval time.Duration
	// Buffer bounds how many events may queue for a slow subscriber.
	Buffer int
	// FullWait is how long Emit blocks for buffer space before declaring the
	// subscriber a slow consumer.
	FullWait time.Duration
}

func (cfg *Config) normalize() {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Millisecond
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 256
	}
	if cfg.FullWait <= 0 {
		cfg.FullWait = 2 * time.Second
	}
}

// Run starts one independent subscription. It returns immediately; events
// stream from the sink and at most one terminal condition (or nothing, when
// ctx ends) arrives on fatalCh.
func Run(ctx context.Context, cfg Config, start logdir.Cursor, resuming bool) (*ChanSink, <-chan *logdir.FatalError) {
	cfg.normalize()
	sink := NewChanSink(cfg.Buffer, cfg.FullWait)
	fatalCh := make(chan *logdir.FatalError, 1)
	go func() {
		f := &follower{
			cfg:      cfg,
			sink:     sink,
			resuming: resuming,
			runID:    start.RunID,
			seg:      start.Segment,
			readOff:  start.Offset,
			delOff:   start.Offset,
		}
		fe := f.loop(ctx)
		// The loop has fully stopped, so no Emit can follow: closing the
		// event channel here (rather than from the HTTP handler) removes the
		// close-vs-send race. fatal is published after the channel closes so
		// the SSE writer's drain pass always sees a closed channel.
		sink.Close()
		if fe != nil {
			fatalCh <- fe
		}
		close(fatalCh)
	}()
	return sink, fatalCh
}

type follower struct {
	cfg      Config
	sink     Sink
	resuming bool

	runID string
	seg   int

	// readOff is the high-water mark of bytes fetched from the open segment
	// (start of the next ReadAt). delOff is the resumable position: the byte
	// after the last delivered newline. Pending half-line bytes live in
	// (delOff, readOff] and are held exactly once in pending.
	readOff int64
	delOff  int64

	f       *os.File
	id      logdir.Identity
	pending []byte
}

// deliverable returns the current resumable cursor.
func (f *follower) deliverable() logdir.Cursor {
	return logdir.Cursor{RunID: f.runID, Segment: f.seg, Offset: f.delOff}
}

func (f *follower) loop(ctx context.Context) *logdir.FatalError {
	for {
		advanced, fe := f.step(ctx)
		if fe != nil {
			return fe
		}
		if ctx.Err() != nil {
			return nil
		}
		if advanced {
			continue // drain sealed successor segments without a poll delay
		}
		t := time.NewTimer(f.cfg.PollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// step performs one directory scan / read pass. advanced=true means it moved
// onto the next segment and the caller should immediately re-run.
func (f *follower) step(ctx context.Context) (bool, *logdir.FatalError) {
	scan, err := logdir.ScanDir(f.cfg.Dir)
	if err != nil {
		return false, f.fatal(logdir.KindInternalError, fmt.Sprintf("cannot scan log directory: %v", err))
	}

	if f.f == nil {
		if !f.resuming {
			// Fresh subscribers start at the oldest retained segment and read
			// forward; they wait for a usable producer instead of anchoring to
			// whichever segment appears first later.
			if scan.RunID == "" || len(scan.Segments) == 0 {
				return false, nil
			}
			f.runID = scan.RunID
			f.seg = scan.Segments[0].Num
		}
		if fe := f.openCurrent(scan); fe != nil {
			return false, fe
		}
	}

	// The run marker is authoritative for the lifetime of a subscription:
	// a different run means the byte stream is unrelated.
	if scan.RunID != f.runID {
		return false, f.fatalf(logdir.KindRunChanged,
			"run id changed from %q to %q", f.runID, scan.RunID)
	}

	highest, ok := scan.Highest()
	if !ok {
		return false, f.fatal(logdir.KindSegmentMissing,
			"every segment disappeared while following")
	}

	scanID, present := segmentID(scan, f.seg)
	if !present {
		if highest.Num > f.seg {
			if gap, yes := scan.MissingBetween(f.seg+1, highest.Num); yes {
				return false, f.fatalf(logdir.KindSegmentGap,
					"segment gap at %d after followed segment %d disappeared (highest %d)",
					gap, f.seg, highest.Num)
			}
		}
		return false, f.fatalf(logdir.KindSegmentMissing,
			"currently followed segment %d was deleted", f.seg)
	}
	if scanID.Dev != f.id.Dev || scanID.Ino != f.id.Ino {
		// Same name, different inode: rename-over replacement. Following the
		// name would splice the replacement onto our pending half-line.
		return false, f.fatalf(logdir.KindSegmentReplaced,
			"segment %d was replaced (inode changed)", f.seg)
	}

	// Stat the open descriptor: a smaller size means in-place truncation
	// (copy-truncate style), which this service deliberately does not follow.
	live, err := logdir.StatIdentity(f.f)
	if err != nil {
		return false, f.fatal(logdir.KindInternalError, fmt.Sprintf("stat open segment: %v", err))
	}
	if live.Size < f.readOff {
		return false, f.fatalf(logdir.KindSegmentReplaced,
			"segment %d shrank behind read position %d (now %d bytes)", f.seg, f.readOff, live.Size)
	}

	if live.Size > f.readOff {
		if fe := f.readTo(ctx, live.Size); fe != nil {
			return false, fe
		}
		if ctx.Err() != nil {
			return false, nil
		}
	}

	if f.seg >= highest.Num {
		// Active (highest) segment: a trailing half-line simply waits.
		return false, nil
	}

	// Current segment is sealed. Pick up bytes appended in the same window as
	// the rollover, then a missing trailing newline is evidence loss.
	final, err := logdir.StatIdentity(f.f)
	if err != nil {
		return false, f.fatal(logdir.KindInternalError, fmt.Sprintf("stat sealed segment: %v", err))
	}
	if final.Size > f.readOff {
		if fe := f.readTo(ctx, final.Size); fe != nil {
			return false, fe
		}
	}
	if len(f.pending) > 0 {
		return false, f.fatalf(logdir.KindTruncated,
			"sealed segment %d ends without a newline; %d trailing bytes are not a complete record",
			f.seg, len(f.pending))
	}

	next := f.seg + 1
	if gap, yes := scan.MissingBetween(next, highest.Num); yes {
		return false, f.fatalf(logdir.KindSegmentGap,
			"segment gap at %d while advancing from %d to %d", gap, f.seg, highest.Num)
	}
	// Entering the next segment: its bytes are read from zero.
	if fe := f.switchTo(next, 0); fe != nil {
		return false, fe
	}
	return true, nil
}

// readTo fetches bytes up to size from the open segment and feeds them to the
// line splitter. ReadAt never shares state across calls.
func (f *follower) readTo(ctx context.Context, size int64) *logdir.FatalError {
	need := size - f.readOff
	if need <= 0 {
		return nil
	}
	buf := make([]byte, need)
	n, err := f.f.ReadAt(buf, f.readOff)
	if n > 0 {
		f.readOff += int64(n)
		if fe := f.deliverBytes(ctx, buf[:n]); fe != nil {
			return fe
		}
	}
	if n < int(need) {
		// File changed between stat and read. Nothing is lost: fetched bytes
		// advanced readOff; the next pass re-stats identity/size and
		// classifies it. Only a hard descriptor error is fatal here.
		if n == 0 && err != nil && !errors.Is(err, io.EOF) {
			return f.fatal(logdir.KindInternalError, fmt.Sprintf("read segment: %v", err))
		}
	}
	return nil
}

// deliverBytes splits strictly on '\n' bytes. A UTF-8 multibyte sequence cut by
// an append stays inside pending until the rest, and its newline, arrives.
func (f *follower) deliverBytes(ctx context.Context, chunk []byte) *logdir.FatalError {
	// pending occupies file bytes [delOff, readOff-len(chunk)]; data starts
	// at delOff, so a newline at data index i ends a record at delOff+i+1.
	dataStart := f.delOff
	// Allocate fresh and copy both parts: append(pending, chunk...) would
	// alias pending's backing array, and the later pending update would then
	// overlap its own source/destination — corrupting half-held UTF-8 bytes.
	data := make([]byte, 0, len(f.pending)+len(chunk))
	data = append(data, f.pending...)
	data = append(data, chunk...)
	consumed := 0
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		lineEnd := dataStart + int64(consumed+i+1)
		// Copy the line: data is reused as it advances across lines.
		line := append([]byte(nil), data[:i]...)
		if fe := f.deliverLine(ctx, line, lineEnd); fe != nil {
			// Subscription ends; do not advance delOff past undelivered
			// records. Retain the unsplit tail including this record.
			f.delOff = dataStart + int64(consumed)
			f.pending = append(f.pending[:0], data...)
			return fe
		}
		data = data[i+1:]
		consumed += i + 1
	}
	f.delOff = dataStart + int64(consumed)
	f.pending = append(f.pending[:0], data...)
	return nil
}

func (f *follower) deliverLine(ctx context.Context, line []byte, endOffset int64) *logdir.FatalError {
	ev := logdir.Event{
		RunID:   f.runID,
		Segment: f.seg,
		Offset:  endOffset,
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, line); err == nil {
		ev.Kind = logdir.KindRecord
		ev.Record = append(json.RawMessage(nil), compact.Bytes()...)
	} else {
		// Complete line, bad JSON: non-fatal error event, later lines survive.
		ev.Kind = logdir.KindBadJSON
		ev.Line = append([]byte(nil), line...)
		ev.Error = err.Error()
	}
	if err := f.sink.Emit(ctx, ev); err != nil {
		if errors.Is(err, ErrSlowConsumer) {
			return f.fatalf(logdir.KindSlowConsumer,
				"subscriber exceeded the %d-event buffer for longer than %s", f.cfg.Buffer, f.cfg.FullWait)
		}
		return nil // context ended; loop exits quietly
	}
	return nil
}

func (f *follower) openCurrent(scan *logdir.Scan) *logdir.FatalError {
	if !scan.Has(f.seg) {
		if highest, ok := scan.Highest(); ok && highest.Num > f.seg {
			if gap, yes := scan.MissingBetween(f.seg+1, highest.Num); yes {
				return f.fatalf(logdir.KindSegmentGap,
					"segment gap at %d between %d and highest %d", gap, f.seg, highest.Num)
			}
		}
		return f.fatalf(logdir.KindSegmentMissing, "segment %d no longer exists", f.seg)
	}
	// Initial open: constructor already set readOff/delOff to the validated
	// cursor offset, so preserve them.
	return f.switchTo(f.seg, f.delOff)
}

// switchTo opens segment n read-only and adopts initialOff as its read
// position (zero when advancing, the cursor offset when resuming).
func (f *follower) switchTo(n int, initialOff int64) *logdir.FatalError {
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	file, err := os.Open(logdir.SegmentPath(f.cfg.Dir, n))
	if err != nil {
		if os.IsNotExist(err) {
			return f.fatalf(logdir.KindSegmentMissing, "segment %d cannot be opened: %v", n, err)
		}
		return f.fatal(logdir.KindInternalError, fmt.Sprintf("open segment %d: %v", n, err))
	}
	id, err := logdir.StatIdentity(file)
	if err != nil {
		_ = file.Close()
		return f.fatal(logdir.KindInternalError, fmt.Sprintf("identify segment %d: %v", n, err))
	}
	f.f = file
	f.id = id
	f.seg = n
	f.readOff = initialOff
	f.delOff = initialOff
	f.pending = f.pending[:0]
	return nil
}

func segmentID(scan *logdir.Scan, n int) (logdir.Identity, bool) {
	for _, seg := range scan.Segments {
		if seg.Num == n {
			return seg.ID, true
		}
	}
	return logdir.Identity{}, false
}

func (f *follower) fatal(kind, msg string) *logdir.FatalError {
	return &logdir.FatalError{Kind: kind, Message: msg, Cursor: f.deliverable()}
}

func (f *follower) fatalf(kind, format string, args ...any) *logdir.FatalError {
	return f.fatal(kind, fmt.Sprintf(format, args...))
}
