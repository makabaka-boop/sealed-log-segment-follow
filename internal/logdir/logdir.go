// Package logdir describes the on-disk layout followed by the log-following
// service: NDJSON files named segment-NNNN.ndjson, a single text file holding
// the run id of the log producer, versioned byte-offset cursors, and the
// event/fatal types shared by the follower and the SSE layer.
package logdir

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

const (
	// MaxSegments bounds how many segment files a conforming producer keeps.
	MaxSegments = 6
	// MaxSegmentSize bounds one segment file to 8 KiB.
	MaxSegmentSize = 8 * 1024
	// FirstSegment is the number of the very first segment file.
	FirstSegment = 1
	// RunIDFile is the marker file holding the producer's run identifier.
	RunIDFile = "runtime.id"

	segmentPrefix = "segment-"
	segmentSuffix = ".ndjson"
)

var segmentNameRE = regexp.MustCompile(`^segment-(\d{4})\.ndjson$`)

// SegmentName renders the canonical file name of a numbered segment.
func SegmentName(n int) string {
	return fmt.Sprintf("%s%04d%s", segmentPrefix, n, segmentSuffix)
}

// ParseSegmentName reports the number carried by a segment file name.
// Strict four-digit naming avoids silently treating unrelated files as logs.
func ParseSegmentName(name string) (int, bool) {
	m := segmentNameRE.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < FirstSegment {
		return 0, false
	}
	return n, true
}

// Identity identifies a file independently of its name. Identity changes when
// a same-named file is replaced (rename-over / copy-truncate style rotation),
// which watching the name alone would miss.
type Identity struct {
	Dev  uint64
	Ino  uint64
	Size int64
}

// StatIdentity collects the identity and current size of an open file.
func StatIdentity(f *os.File) (Identity, error) {
	fi, err := f.Stat()
	if err != nil {
		return Identity{}, err
	}
	sys, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, fmt.Errorf("unsupported filesystem metadata for %q", f.Name())
	}
	return Identity{Dev: uint64(sys.Dev), Ino: uint64(sys.Ino), Size: fi.Size()}, nil
}

// LstatIdentity collects identity and size from a path's directory entry
// without opening it: the metadata reflects whatever currently occupies the
// name, which is exactly what replacement detection needs.
func LstatIdentity(path string) (Identity, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return Identity{}, err
	}
	sys, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, fmt.Errorf("unsupported filesystem metadata for %q", path)
	}
	if !fi.Mode().IsRegular() {
		return Identity{}, fmt.Errorf("%s is not a regular file", path)
	}
	return Identity{Dev: uint64(sys.Dev), Ino: uint64(sys.Ino), Size: fi.Size()}, nil
}

// Segment is one discovered segment file at the instant of a directory scan.
type Segment struct {
	Num int
	ID  Identity
}

// Scan is a consistent snapshot of the log directory.
type Scan struct {
	RunID    string
	Segments []Segment // sorted ascending by Num
}

// ScanDir reads the directory and validates the producer's layout.
func ScanDir(dir string) (*Scan, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Scan{}
	for _, e := range entries {
		switch name := e.Name(); {
		case name == RunIDFile:
			if e.IsDir() {
				return nil, fmt.Errorf("%s must be a regular file", RunIDFile)
			}
			raw, err := os.ReadFile(dirPath(dir, name))
			if err != nil {
				return nil, err
			}
			s.RunID = strings.TrimSpace(string(raw))
		default:
			n, ok := ParseSegmentName(name)
			if !ok {
				continue // unrelated files are ignored
			}
			if e.IsDir() {
				return nil, fmt.Errorf("%s must be a regular file", name)
			}
			// Identity comes from lstat on the directory ENTRY, never from a
			// fresh open(). After a rename-over replacement, reopening yields
			// the replacement's inode and would wrongly match a follower that
			// holds the pre-replacement descriptor; the entry's d_ino-style
			// metadata changes and exposes the swap.
			id, statErr := LstatIdentity(dirPath(dir, name))
			if statErr != nil {
				return nil, statErr
			}
			s.Segments = append(s.Segments, Segment{Num: n, ID: id})
		}
	}
	// ReadDir order is unspecified; sort without a sort.Slice allocation loop.
	for i := 1; i < len(s.Segments); i++ {
		for j := i; j > 0 && s.Segments[j-1].Num > s.Segments[j].Num; j-- {
			s.Segments[j-1], s.Segments[j] = s.Segments[j], s.Segments[j-1]
		}
	}
	return s, nil
}

// Has reports whether the snapshot contains segment n.
func (s *Scan) Has(n int) bool {
	for _, seg := range s.Segments {
		if seg.Num == n {
			return true
		}
	}
	return false
}

// Highest returns the highest numbered segment, or false when none exist yet.
func (s *Scan) Highest() (Segment, bool) {
	if len(s.Segments) == 0 {
		return Segment{}, false
	}
	return s.Segments[len(s.Segments)-1], true
}

// MissingBetween returns the first absent number in [from,to], or 0/false
// when the whole range is present.
func (s *Scan) MissingBetween(from, to int) (int, bool) {
	if to < from {
		return 0, false
	}
	present := make(map[int]struct{}, len(s.Segments))
	for _, seg := range s.Segments {
		present[seg.Num] = struct{}{}
	}
	for n := from; n <= to; n++ {
		if _, ok := present[n]; !ok {
			return n, true
		}
	}
	return 0, false
}

func dirPath(dir, name string) string {
	if dir == "" {
		return name
	}
	if strings.HasSuffix(dir, "/") {
		return dir + name
	}
	return dir + "/" + name
}

// SegmentPath is the full path of a numbered segment inside dir.
func SegmentPath(dir string, n int) string { return dirPath(dir, SegmentName(n)) }

// RunIDPath is the full path of the run-id marker inside dir.
func RunIDPath(dir string) string { return dirPath(dir, RunIDFile) }

// Cursor is the resumption position: run id, segment number and the byte
// offset immediately after the last fully delivered record's newline.
type Cursor struct {
	RunID   string `json:"run"`
	Segment int    `json:"seg"`
	Offset  int64  `json:"off"`
}

const cursorVersion = "v1:"

// EncodeCursor renders the opaque token used in SSE id / Last-Event-ID.
// It is version-prefixed and base64url so it can never contain a newline.
func EncodeCursor(c Cursor) string {
	raw, _ := json.Marshal(c)
	return cursorVersion + base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor parses a token previously produced by EncodeCursor.
func DecodeCursor(tok string) (Cursor, error) {
	if !strings.HasPrefix(tok, cursorVersion) {
		return Cursor{}, errors.New("cursor: unsupported version")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, cursorVersion))
	if err != nil {
		return Cursor{}, fmt.Errorf("cursor: malformed token: %w", err)
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return Cursor{}, fmt.Errorf("cursor: malformed payload: %w", err)
	}
	if c.RunID == "" || c.Segment < FirstSegment || c.Offset < 0 {
		return Cursor{}, errors.New("cursor: invalid fields")
	}
	return c, nil
}

// Kind values classify fatal and non-fatal events emitted to a subscriber.
const (
	// KindRecord is a complete, syntactically valid NDJSON line.
	KindRecord = "record"
	// KindBadJSON is a complete newline-terminated line that failed to parse.
	// It is non-fatal: following continues with the next line.
	KindBadJSON = "bad_json"
	// Fatal kinds: exactly one of these terminates the subscription.
	KindTruncated       = "truncated"
	KindSegmentGap      = "segment_gap"
	KindSegmentMissing  = "segment_missing"
	KindSegmentReplaced = "segment_replaced"
	KindRunChanged      = "run_changed"
	KindSlowConsumer    = "slow_consumer"
	KindInternalError   = "internal_error"
)

// FatalError is the terminal condition of a subscription.
type FatalError struct {
	Kind    string
	Message string
	Cursor  Cursor // best-known position; reference only for fatal kinds
}

func (e *FatalError) Error() string { return e.Kind + ": " + e.Message }

// Event is one SSE-worthy delivery. Fatal conditions travel back through the
// Run error channel instead of this type.
type Event struct {
	Kind    string
	RunID   string
	Segment int
	// Offset is the byte offset of the end of the line (position after '\n'),
	// i.e. where resumption must start.
	Offset int64
	// Record is the canonicalised JSON for KindRecord.
	Record json.RawMessage
	// Line is the raw complete line, used for KindBadJSON.
	Line []byte
	// Error is the parse error message for KindBadJSON.
	Error string
}

// Cursor returns the resumption token position after this event.
func (e Event) Cursor() Cursor {
	return Cursor{RunID: e.RunID, Segment: e.Segment, Offset: e.Offset}
}

// Ensure FileMode is used as little as possible; helper for tests/tools.
func RegularMode() fs.FileMode { return 0o644 }
