package follow

import (
	"testing"

	"logfollow/internal/logdir"
)

// TestResumeCheckBoundary is the byte-evidence test: only an offset whose
// preceding on-disk byte is '\n' (or 0) is accepted; mid-line and rune-count
// positions are rejected.
func TestResumeCheckBoundary(t *testing.T) {
	dir := setupDir(t, "run-bound")
	// Bytes: {"a":"世"}\n  (multibyte rune; 12 bytes total incl newline)
	line := `{"a":"世"}` + "\n"
	appendLine(t, dir, 1, line)
	end := int64(len(line))

	cases := []struct {
		name   string
		off    int64
		reason string // "" means accepted
	}{
		{"start", 0, ""},
		{"end newline boundary", end, ""},
		{"one byte before end", end - 1, ResumeNotLineBoundary},
		{"mid multibyte byte", 8, ResumeNotLineBoundary},
		{"beyond size", end + 1, ResumeOffsetOutOfRange},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := logdir.Cursor{RunID: "run-bound", Segment: 1, Offset: tc.off}
			re := ResumeCheck(dir, c)
			if tc.reason == "" {
				if re != nil {
					t.Fatalf("expected accept, got %v", re)
				}
				return
			}
			if re == nil {
				t.Fatalf("expected %s, got acceptance", tc.reason)
			}
			if re.Reason != tc.reason {
				t.Fatalf("reason = %s, want %s", re.Reason, tc.reason)
			}
		})
	}

	// Run mismatch.
	if re := ResumeCheck(dir, logdir.Cursor{RunID: "OTHER", Segment: 1, Offset: 0}); re == nil || re.Reason != ResumeRunMismatch {
		t.Fatalf("run check = %v", re)
	}
	// Missing segment.
	if re := ResumeCheck(dir, logdir.Cursor{RunID: "run-bound", Segment: 9, Offset: 0}); re == nil || re.Reason != ResumeSegmentMissing {
		t.Fatalf("missing check = %v", re)
	}
}

// TestResumeCheckGap: contiguous history accepted, a hole rejected.
func TestResumeCheckGap(t *testing.T) {
	dir := setupDir(t, "run-gap")
	appendLine(t, dir, 1, `{"n":1}`+"\n")
	mk2(t, dir)
	mk3(t, dir)

	c := logdir.Cursor{RunID: "run-gap", Segment: 1, Offset: 8}
	if re := ResumeCheck(dir, c); re != nil {
		t.Fatalf("contiguous resume rejected: %v", re)
	}
	rm2(t, dir)
	if re := ResumeCheck(dir, c); re == nil || re.Reason != ResumeSegmentGap {
		t.Fatalf("gap check = %v, want segment_gap", re)
	}
}

func mk2(t *testing.T, dir string) {
	t.Helper()
	if err := writeEmpty(logdir.SegmentPath(dir, 2)); err != nil {
		t.Fatal(err)
	}
}

func mk3(t *testing.T, dir string) {
	t.Helper()
	if err := writeEmpty(logdir.SegmentPath(dir, 3)); err != nil {
		t.Fatal(err)
	}
}

func rm2(t *testing.T, dir string) {
	t.Helper()
	if err := removeSeg(logdir.SegmentPath(dir, 2)); err != nil {
		t.Fatal(err)
	}
}
