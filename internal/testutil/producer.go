// Package testutil provides a reference log producer used by the test suite:
// it honours the layout contract — at most MaxSegments segment files, each at
// most MaxSegmentSize bytes, only the highest-numbered segment writable — by
// sealing and opening the next segment.
package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"logfollow/internal/logdir"
)

// Producer appends NDJSON bytes into a segment directory and rolls segments
// at the 8 KiB boundary, refusing to create more than six segments.
type Producer struct {
	mu  sync.Mutex
	dir string

	runID string
	seg   int
	size  int64
}

// NewProducer creates (or reuses) dir, writes the run-id marker and opens
// segment-0001 for appending.
func NewProducer(dir, runID string) (*Producer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, logdir.RunIDFile), []byte(runID+"\n"), 0o644); err != nil {
		return nil, err
	}
	p := &Producer{dir: dir, runID: runID, seg: 1}
	if err := os.WriteFile(logdir.SegmentPath(dir, 1), nil, 0o644); err != nil {
		return nil, err
	}
	return p, nil
}

// Dir is the managed directory.
func (p *Producer) Dir() string { return p.dir }

// RunID is the producer's current run identifier.
func (p *Producer) RunID() string { return p.runID }

// Segment reports the active (highest) segment number.
func (p *Producer) Segment() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seg
}

// SegmentSize reports bytes already written to the active segment.
func (p *Producer) SegmentSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// Append writes raw bytes to the active segment without rolling, even across
// the size limit (tests use it to inject half-lines and split UTF-8 bytes).
// It fails if the six-segment ceiling would be exceeded by a later Roll.
func (p *Producer) Append(b []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.OpenFile(logdir.SegmentPath(p.dir, p.seg), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := f.Write(b)
	p.size += int64(n)
	return err
}

// AppendLine appends a complete newline-terminated line.
func (p *Producer) AppendLine(line []byte) error {
	if len(line) > 0 && line[len(line)-1] != '\n' {
		line = append(append([]byte(nil), line...), '\n')
	}
	return p.Append(line)
}

// AppendJSON appends a JSON object line: {"seq":N,"msg":"..."}.
func (p *Producer) AppendJSON(seq int, msg string) error {
	return p.AppendLine([]byte(fmt.Sprintf(`{"seq":%d,"msg":%q}`, seq, msg)))
}

// AppendJSONBytes appends an already-encoded JSON value as one line.
func (p *Producer) AppendJSONBytes(raw []byte) error {
	return p.AppendLine(raw)
}

// AppendLineWithRoll writes a line, rolling over to the next sealed/active
// pair whenever the write would cross 8 KiB. It refuses to open a segment
// above MaxSegments.
func (p *Producer) AppendLineWithRoll(line []byte) error {
	if len(line) > 0 && line[len(line)-1] != '\n' {
		line = append(append([]byte(nil), line...), '\n')
	}
	p.mu.Lock()
	if int64(len(line)) > logdir.MaxSegmentSize {
		p.mu.Unlock()
		return fmt.Errorf("testutil: line of %d bytes exceeds segment limit %d", len(line), logdir.MaxSegmentSize)
	}
	if p.size+int64(len(line)) > logdir.MaxSegmentSize {
		p.mu.Unlock()
		if err := p.Roll(); err != nil {
			return err
		}
	} else {
		p.mu.Unlock()
	}
	return p.Append(line)
}

// Roll seals the active segment and creates its successor. The old file is
// never modified again.
func (p *Producer) Roll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seg >= logdir.MaxSegments {
		return fmt.Errorf("testutil: segment ceiling (%d) reached", logdir.MaxSegments)
	}
	p.seg++
	p.size = 0
	return os.WriteFile(logdir.SegmentPath(p.dir, p.seg), nil, 0o644)
}

// ReplaceActive atomically replaces the active segment file with new bytes
// under the same name (rename-over). A name-only watcher misses this.
func (p *Producer) ReplaceActive(newContent []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmp := logdir.SegmentPath(p.dir, p.seg) + ".tmp"
	if err := os.WriteFile(tmp, newContent, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, logdir.SegmentPath(p.dir, p.seg)); err != nil {
		return err
	}
	p.size = int64(len(newContent))
	return nil
}

// TruncateActive cuts the active segment in place (copy-truncate emulation),
// which this service deliberately refuses to follow.
func (p *Producer) TruncateActive(size int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := os.Truncate(logdir.SegmentPath(p.dir, p.seg), size); err != nil {
		return err
	}
	p.size = size
	return nil
}

// SetRunID rewrites the run marker, emulating a producer restart with a new
// run identifier.
func (p *Producer) SetRunID(runID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runID = runID
	return os.WriteFile(filepath.Join(p.dir, logdir.RunIDFile), []byte(runID+"\n"), 0o644)
}

// Delete removes a segment file (used to exercise missing-history resumes).
func (p *Producer) Delete(n int) error {
	return os.Remove(logdir.SegmentPath(p.dir, n))
}

// Path returns the absolute path of a segment file.
func (p *Producer) Path(n int) string { return logdir.SegmentPath(p.dir, n) }
