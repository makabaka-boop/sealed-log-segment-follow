package logdir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentNameRoundTrip(t *testing.T) {
	for _, n := range []int{1, 2, 6, 42, 9999} {
		name := SegmentName(n)
		got, ok := ParseSegmentName(name)
		if !ok || got != n {
			t.Fatalf("round trip %d -> %q -> %d,%v", n, name, got, ok)
		}
	}
	for _, bad := range []string{
		"segment-0000.ndjson", // zero is not a valid number
		"segment-001.ndjson",  // not four digits
		"segment-00001.ndjson",
		"segment-abcd.ndjson",
		"segment-0001.log",
		"xsegment-0001.ndjson",
		".segment-0001.ndjson",
		"",
	} {
		if _, ok := ParseSegmentName(bad); ok {
			t.Fatalf("ParseSegmentName(%q) unexpectedly accepted", bad)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := Cursor{RunID: "run-代号/1", Segment: 42, Offset: 8192}
	tok := EncodeCursor(c)
	got, err := DecodeCursor(tok)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != c {
		t.Fatalf("cursor mismatch: %+v vs %+v", got, c)
	}
	// Tokens never carry a newline (they are SSE id lines).
	for _, b := range []byte(tok) {
		if b == '\n' || b == '\r' {
			t.Fatalf("cursor token contains line break: %q", tok)
		}
	}
}

func TestCursorRejectsGarbage(t *testing.T) {
	for _, bad := range []string{
		"",
		"v1:",
		"v2:aaaa",
		"garbage",
		"v1:!!!",
	} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Fatalf("DecodeCursor(%q) unexpectedly accepted", bad)
		}
	}
}

func TestScanLayout(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, RunIDFile), []byte("  r1 \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(SegmentPath(dir, 3), []byte("ccc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(SegmentPath(dir, 1), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Unrelated files must be ignored, not error.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	scan, err := ScanDir(dir)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if scan.RunID != "r1" {
		t.Fatalf("run id = %q, want trimmed r1", scan.RunID)
	}
	if len(scan.Segments) != 2 || scan.Segments[0].Num != 1 || scan.Segments[1].Num != 3 {
		t.Fatalf("segments = %+v, want sorted [1 3]", scan.Segments)
	}
	if scan.Segments[0].ID.Size != 1 || scan.Segments[1].ID.Size != 3 {
		t.Fatalf("sizes = %d,%d", scan.Segments[0].ID.Size, scan.Segments[1].ID.Size)
	}
	if scan.Segments[0].ID.Ino == 0 {
		t.Fatal("inode should be populated")
	}
	if h, ok := scan.Highest(); !ok || h.Num != 3 {
		t.Fatalf("highest = %+v,%v", h, ok)
	}
	if g, yes := scan.MissingBetween(1, 3); !yes || g != 2 {
		t.Fatalf("gap = %d,%v want 2", g, yes)
	}
	if _, yes := scan.MissingBetween(3, 3); yes {
		t.Fatal("3..3 should not be a gap")
	}
}
