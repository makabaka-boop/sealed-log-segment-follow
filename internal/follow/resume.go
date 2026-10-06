package follow

import (
	"errors"
	"fmt"
	"io"
	"os"

	"logfollow/internal/logdir"
)

// Resume error reasons. These are protocol-level rejections: resumption is
// impossible and the client must not be silently moved to the current tail.
const (
	ResumeMalformed        = "malformed_cursor"
	ResumeRunMismatch      = "run_mismatch"
	ResumeSegmentMissing   = "segment_missing"
	ResumeSegmentGap       = "segment_gap"
	ResumeNotLineBoundary  = "not_line_boundary"
	ResumeOffsetOutOfRange = "offset_out_of_range"
)

// ResumeError is a pre-flight resumption failure with a stable machine reason.
type ResumeError struct {
	Reason  string
	Message string
	Cursor  logdir.Cursor
}

func (e *ResumeError) Error() string { return e.Reason + ": " + e.Message }

// ResumeCheck validates a resumption cursor against the directory before any
// events are streamed:
//
//   - run id must match the current producer marker;
//   - the cursor segment must still exist;
//   - numbers between it and the highest segment must be contiguous;
//   - the offset must be in range and sit at a real newline boundary in the
//     actual bytes of that file — never reconstructed from character counts.
//
// Offset 0 is a valid boundary (start of the segment).
func ResumeCheck(dir string, c logdir.Cursor) *ResumeError {
	scan, err := logdir.ScanDir(dir)
	if err != nil {
		return &ResumeError{Reason: ResumeSegmentMissing, Message: fmt.Sprintf("scan log directory: %v", err), Cursor: c}
	}
	if scan.RunID != c.RunID {
		return &ResumeError{
			Reason:  ResumeRunMismatch,
			Message: fmt.Sprintf("run id %q does not match current producer %q", c.RunID, orEmpty(scan.RunID)),
			Cursor:  c,
		}
	}
	seg, ok := findSegment(scan, c.Segment)
	if !ok {
		return &ResumeError{
			Reason:  ResumeSegmentMissing,
			Message: fmt.Sprintf("cursor segment %d no longer exists", c.Segment),
			Cursor:  c,
		}
	}
	highest, _ := scan.Highest()
	if c.Segment < highest.Num {
		if gap, yes := scan.MissingBetween(c.Segment, highest.Num); yes {
			return &ResumeError{
				Reason:  ResumeSegmentGap,
				Message: fmt.Sprintf("segment number gap at %d between cursor segment %d and highest %d", gap, c.Segment, highest.Num),
				Cursor:  c,
			}
		}
	}
	if c.Offset > seg.ID.Size {
		return &ResumeError{
			Reason:  ResumeOffsetOutOfRange,
			Message: fmt.Sprintf("offset %d beyond size %d of segment %d", c.Offset, seg.ID.Size, c.Segment),
			Cursor:  c,
		}
	}
	if c.Offset > 0 {
		if err := checkNewlineBoundary(dir, c.Segment, c.Offset); err != nil {
			return &ResumeError{
				Reason:  ResumeNotLineBoundary,
				Message: err.Error(),
				Cursor:  c,
			}
		}
	}
	return nil
}

// checkNewlineBoundary confirms byte (offset-1) is '\n' on disk. Rune-aware
// arithmetic cannot produce a valid cursor: only delivered line ends can.
func checkNewlineBoundary(dir string, segment int, offset int64) error {
	f, err := os.Open(logdir.SegmentPath(dir, segment))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("segment %d vanished before boundary check", segment)
		}
		return fmt.Errorf("open segment %d: %w", segment, err)
	}
	defer f.Close()

	var one [1]byte
	n, err := f.ReadAt(one[:], offset-1)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read boundary byte of segment %d at %d: %w", segment, offset-1, err)
	}
	if n != 1 {
		return fmt.Errorf("offset %d not readable in segment %d", offset, segment)
	}
	if one[0] != '\n' {
		return fmt.Errorf("offset %d is not a newline boundary in segment %d (preceding byte 0x%02x)",
			offset, segment, one[0])
	}
	return nil
}

func findSegment(scan *logdir.Scan, n int) (logdir.Segment, bool) {
	for _, seg := range scan.Segments {
		if seg.Num == n {
			return seg, true
		}
	}
	return logdir.Segment{}, false
}

func orEmpty(s string) string {
	if s == "" {
		return "<missing>"
	}
	return s
}
