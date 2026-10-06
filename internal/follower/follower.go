package follower

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	MaxSegments    = 6
	MaxSegmentSize = 8 * 1024
)

// Error codes are part of the client-visible SSE/HTTP contract.
const (
	CodeInvalidCursor    = "invalid_cursor"
	CodeRunChanged       = "run_id_changed"
	CodeSegmentDeleted   = "cursor_segment_deleted"
	CodeSegmentGap       = "segment_gap"
	CodeOffsetBoundary   = "offset_not_newline_boundary"
	CodeOffsetBeyondEOF  = "offset_beyond_eof"
	CodeSealedTruncated  = "sealed_segment_without_final_newline"
	CodeTooManySegments  = "too_many_segments"
	CodeSegmentOversized = "segment_too_large"
	CodeSegmentReplaced  = "segment_replaced"
	CodeSegmentTruncated = "active_segment_truncated"
	CodeSealedModified   = "sealed_segment_modified"
	CodeReadFailed       = "read_failed"
	CodeSlowConsumer     = "slow_consumer"
	CodeCanceled         = "canceled"
)

var segmentNameRE = regexp.MustCompile(`^segment-(\d{4})\.ndjson$`)

// FollowError describes a condition that prevents following or makes a
// subscription unable to continue.
type FollowError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Segment int    `json:"segment,omitempty"`
	Offset  int64  `json:"offset,omitempty"`
	Err     error  `json:"-"`
}

func (e *FollowError) Error() string {
	if e == nil {
		return ""
	}
	s := e.Code + ": " + e.Message
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *FollowError) Unwrap() error { return e.Err }

func followError(code, message string, segment int, offset int64, cause error) *FollowError {
	return &FollowError{Code: code, Message: message, Segment: segment, Offset: offset, Err: cause}
}

// ErrorDetail is delivered in an SSE error event.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Event is a completed SSE data record. Invalid but newline-delimited lines
// are also delivered as events, with Error populated and the exact offending
// bytes base64-encoded in Raw (invalid UTF-8 must not be silently rewritten).
type Event struct {
	Type      string          `json:"type"`
	RunID     string          `json:"run_id"`
	Segment   int             `json:"segment"`
	EndOffset int64           `json:"end_offset"`
	Result    json.RawMessage `json:"result,omitempty"`
	Raw       []byte          `json:"raw_b64,omitempty"`
	Error     *ErrorDetail    `json:"error,omitempty"`
}

type TerminalEvent struct {
	Type    string       `json:"type"`
	RunID   string       `json:"run_id"`
	Segment int          `json:"segment,omitempty"`
	Offset  int64        `json:"offset,omitempty"`
	Error   *ErrorDetail `json:"error"`
}

// Cursor is the JSON payload encoded into Last-Event-ID / id:.
type Cursor struct {
	Version int    `json:"v"`
	RunID   string `json:"run_id"`
	Segment int    `json:"segment"`
	Offset  int64  `json:"offset"`
}

const cursorVersion = 1

// EncodeCursor returns the opaque Last-Event-ID value for a complete record.
func EncodeCursor(runID string, segment int, offset int64) string {
	c := Cursor{Version: cursorVersion, RunID: runID, Segment: segment, Offset: offset}
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses an opaque Last-Event-ID value.
func DecodeCursor(value string) (Cursor, error) {
	var c Cursor
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return c, followError(CodeInvalidCursor, "Last-Event-ID is not valid base64url", 0, 0, err)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, followError(CodeInvalidCursor, "Last-Event-ID does not contain a cursor", 0, 0, err)
	}
	if c.Version != cursorVersion || c.RunID == "" || c.Segment <= 0 || c.Offset < 0 {
		return c, followError(CodeInvalidCursor, "cursor has unsupported fields", c.Segment, c.Offset, nil)
	}
	return c, nil
}

type fileIdentity struct {
	dev uint64
	ino uint64
}

type segmentInfo struct {
	seq  int
	path string
	size int64
	id   fileIdentity
}

type knownSegment struct {
	id   fileIdentity
	size int64
}

// Barrier is a test synchronization primitive. A follower marks the beginning
// and end of every directory poll; tests can wait for one complete poll after
// an append/rotation.
type Barrier struct {
	mu        sync.Mutex
	nextID    uint64
	nextWait  uint64
	epochs    map[uint64]uint64
	completed map[uint64]map[uint64]struct{}
	waiters   map[uint64]map[uint64]uint64 // waiter -> follower -> epoch awaited
	notify    map[uint64]chan struct{}
}

func NewBarrier() *Barrier {
	return &Barrier{
		epochs:    make(map[uint64]uint64),
		completed: make(map[uint64]map[uint64]struct{}),
		waiters:   make(map[uint64]map[uint64]uint64),
		notify:    make(map[uint64]chan struct{}),
	}
}

func (b *Barrier) register() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	b.epochs[id] = 0
	b.completed[id] = make(map[uint64]struct{})
	b.mu.Unlock()
	return id
}

func (b *Barrier) unregister(id uint64) {
	if b == nil || id == 0 {
		return
	}
	b.mu.Lock()
	delete(b.epochs, id)
	delete(b.completed, id)
	for _, wants := range b.waiters {
		delete(wants, id)
	}
	b.satisfyLocked()
	b.mu.Unlock()
}

func (b *Barrier) begin(id uint64) func() {
	if b == nil || id == 0 {
		return func() {}
	}
	b.mu.Lock()
	b.epochs[id]++
	epoch := b.epochs[id]
	done := false
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		if !done {
			done = true
			b.completed[id][epoch] = struct{}{}
			b.satisfyLocked()
		}
		b.mu.Unlock()
	}
}

// satisfyLocked closes and removes every waiter whose required epochs have all
// completed (caller holds b.mu).
func (b *Barrier) satisfyLocked() {
ready:
	for waitID, wants := range b.waiters {
		for id, epoch := range wants {
			if epoch == 0 {
				continue
			}
			done, ok := b.completed[id]
			if !ok {
				delete(wants, id) // follower vanished; nothing to wait for
				continue
			}
			if _, ok := done[epoch]; !ok {
				continue ready
			}
		}
		close(b.notify[waitID])
		delete(b.waiters, waitID)
		delete(b.notify, waitID)
	}
}

// Wait blocks until every currently registered follower has completed its
// current poll. It returns immediately when nobody is subscribed.
func (b *Barrier) Wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	wants := make(map[uint64]uint64)
	for id, epoch := range b.epochs {
		if epoch > 0 {
			wants[id] = epoch
		}
	}
	if len(wants) == 0 {
		b.mu.Unlock()
		return nil
	}
	b.nextWait++
	waitID := b.nextWait
	ch := make(chan struct{})
	b.waiters[waitID] = wants
	b.notify[waitID] = ch
	b.satisfyLocked()
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		b.mu.Lock()
		if _, ok := b.waiters[waitID]; ok {
			delete(b.waiters, waitID)
			delete(b.notify, waitID)
		}
		b.mu.Unlock()
		return ctx.Err()
	case <-ch:
		return nil
	}
}

type Config struct {
	Directory    string
	RunID        string
	PollInterval time.Duration
	EventBuffer  int
	Barrier      *Barrier
}

type Follower struct {
	dir     string
	runID   string
	poll    time.Duration
	bufSize int
	barrier *Barrier
}

func New(cfg Config) (*Follower, error) {
	if cfg.Directory == "" {
		return nil, errors.New("follower: directory is required")
	}
	if cfg.RunID == "" {
		return nil, errors.New("follower: run id is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 20 * time.Millisecond
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = 64
	}
	st, err := os.Stat(cfg.Directory)
	if err != nil {
		return nil, fmt.Errorf("follower: log directory: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("follower: log path %q is not a directory", cfg.Directory)
	}
	return &Follower{
		dir:     cfg.Directory,
		runID:   cfg.RunID,
		poll:    cfg.PollInterval,
		bufSize: cfg.EventBuffer,
		barrier: cfg.Barrier,
	}, nil
}

func (f *Follower) RunID() string { return f.runID }

func fileIdentityFromInfo(st os.FileInfo) (fileIdentity, error) {
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("unsupported file info type %T", st.Sys())
	}
	return fileIdentity{dev: uint64(sys.Dev), ino: uint64(sys.Ino)}, nil
}

func (f *Follower) scanSegments() ([]segmentInfo, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, followError(CodeReadFailed, "cannot read log directory", 0, 0, err)
	}
	var found []segmentInfo
	for _, entry := range entries {
		m := segmentNameRE.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		var seq int
		fmt.Sscanf(m[1], "%d", &seq)
		path := filepath.Join(f.dir, entry.Name())
		st, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, followError(CodeReadFailed, "cannot stat segment", seq, 0, err)
		}
		if !st.Mode().IsRegular() {
			continue
		}
		id, err := fileIdentityFromInfo(st)
		if err != nil {
			return nil, followError(CodeReadFailed, "cannot identify segment", seq, 0, err)
		}
		found = append(found, segmentInfo{seq: seq, path: path, size: st.Size(), id: id})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].seq < found[j].seq })
	if len(found) > MaxSegments {
		return nil, followError(CodeTooManySegments,
			fmt.Sprintf("found %d segments; producer may retain at most %d", len(found), MaxSegments),
			found[0].seq, 0, nil)
	}
	for _, s := range found {
		if s.size > MaxSegmentSize {
			return nil, followError(CodeSegmentOversized,
				fmt.Sprintf("segment %d is %d bytes; limit is %d", s.seq, s.size, MaxSegmentSize),
				s.seq, s.size, nil)
		}
	}
	return found, nil
}

func segmentBySeq(ss []segmentInfo, seq int) (segmentInfo, bool) {
	i := sort.Search(len(ss), func(i int) bool { return ss[i].seq >= seq })
	if i < len(ss) && ss[i].seq == seq {
		return ss[i], true
	}
	return segmentInfo{}, false
}

func endsWithNewlineAt(path string, size int64) (bool, error) {
	if size <= 0 {
		return false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	var b [1]byte
	n, err := file.ReadAt(b[:], size-1)
	if n == 1 {
		return b[0] == '\n', nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func validateNewlineBoundary(path string, size, offset int64, segment int) error {
	if offset > size {
		return followError(CodeOffsetBeyondEOF,
			fmt.Sprintf("cursor offset %d is beyond segment size %d", offset, size), segment, offset, nil)
	}
	if offset == 0 {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return followError(CodeReadFailed, "cannot open segment for cursor validation", segment, offset, err)
	}
	defer file.Close()
	var b [1]byte
	n, err := file.ReadAt(b[:], offset-1)
	if n != 1 || err != nil {
		cause := err
		if cause == nil {
			cause = errors.New("short boundary read")
		}
		return followError(CodeReadFailed, "cannot read candidate newline boundary", segment, offset, cause)
	}
	if b[0] != '\n' {
		return followError(CodeOffsetBoundary,
			"cursor offset is not immediately after a real newline byte; UTF-8/character positions are not accepted",
			segment, offset, nil)
	}
	return nil
}

func initialKnown(ss []segmentInfo) map[int]knownSegment {
	m := make(map[int]knownSegment, len(ss))
	for _, s := range ss {
		m[s.seq] = knownSegment{id: s.id, size: s.size}
	}
	return m
}

// Subscription is an independent, bounded subscriber stream.
type Subscription struct {
	events    chan Event
	terminal  chan error
	interrupt chan struct{}
	cancel    context.CancelFunc

	once    sync.Once
	termErr error
}

func (s *Subscription) Events() <-chan Event  { return s.events }
func (s *Subscription) Done() <-chan struct{} { return s.interrupt }
func (s *Subscription) Err() error            { return s.termErr }

func (s *Subscription) stop() {
	s.once.Do(func() { close(s.interrupt) })
}

// Open starts at cursor when non-nil, otherwise at the oldest known segment.
// Resume errors are returned synchronously so the HTTP handler can reject
// Last-Event-ID instead of upgrading to SSE.
func (f *Follower) Open(parent context.Context, cursor *Cursor) (*Subscription, error) {
	var startSeq int
	var startOffset int64
	if cursor != nil {
		if cursor.RunID != f.runID {
			return nil, followError(CodeRunChanged,
				"cursor belongs to a different service run and cannot be replayed", cursor.Segment, cursor.Offset, nil)
		}
		startSeq, startOffset = cursor.Segment, cursor.Offset
	}

	ss, err := f.scanSegments()
	if err != nil {
		return nil, err
	}
	known := initialKnown(ss)
	if cursor != nil {
		if len(ss) == 0 {
			return nil, followError(CodeSegmentDeleted, "cursor segment is absent and directory has no segments", cursor.Segment, cursor.Offset, nil)
		}
		current, ok := segmentBySeq(ss, startSeq)
		if !ok {
			return nil, followError(CodeSegmentDeleted, "cursor segment no longer exists", startSeq, startOffset, nil)
		}
		highest := ss[len(ss)-1].seq
		for seq := startSeq; seq <= highest; seq++ {
			if _, ok := segmentBySeq(ss, seq); !ok {
				return nil, followError(CodeSegmentGap,
					fmt.Sprintf("segment %04d is missing between cursor and highest segment %04d", seq, highest),
					startSeq, startOffset, nil)
			}
		}
		if err := validateNewlineBoundary(current.path, current.size, startOffset, startSeq); err != nil {
			return nil, err
		}
	} else if len(ss) > 0 {
		startSeq = ss[0].seq
		startOffset = 0
		if startSeq != 1 {
			return nil, followError(CodeSegmentGap,
				fmt.Sprintf("oldest segment is %04d but segment-0001 is missing; cannot start from the middle", startSeq),
				startSeq, 0, nil)
		}
	}
	// Reject known malformed sealed history before upgrading. Every segment
	// below the current highest must end on a real newline boundary.
	for _, s := range ss {
		highest := ss[len(ss)-1].seq
		if s.seq < highest {
			ok, err := endsWithNewlineAt(s.path, s.size)
			if err != nil {
				return nil, followError(CodeReadFailed, "cannot validate sealed segment", s.seq, 0, err)
			}
			if !ok {
				return nil, followError(CodeSealedTruncated,
					"sealed segment does not end with a newline; its last record is truncated",
					s.seq, s.size, nil)
			}
		}
	}

	ctx, cancel := context.WithCancel(parent)
	sub := &Subscription{
		events:    make(chan Event, f.bufSize),
		terminal:  make(chan error, 1),
		interrupt: make(chan struct{}),
		cancel:    cancel,
	}
	id := f.barrier.register()
	state := &followState{
		f:      f,
		ctx:    ctx,
		sub:    sub,
		seq:    startSeq,
		offset: startOffset,
		known:  known,
		id:     id,
	}
	go func() {
		defer f.barrier.unregister(id)
		defer cancel()
		err := state.run()
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			err = nil
		}
		if err != nil {
			sub.termErr = err
		} else if ctx.Err() != nil {
			err = followError(CodeCanceled, "client canceled", state.seq, state.offset, ctx.Err())
			sub.termErr = err
		}
		sub.terminal <- err
		sub.stop()
	}()
	return sub, nil
}

type followState struct {
	f       *Follower
	ctx     context.Context
	sub     *Subscription
	seq     int
	offset  int64
	pending []byte
	known   map[int]knownSegment
	id      uint64
}

func (s *followState) run() error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-timer.C:
		}
		finish := s.f.barrier.begin(s.id)
		err := s.poll()
		finish()
		if err != nil {
			return err
		}
		timer.Reset(s.f.poll)
	}
}

func (s *followState) poll() error {
	ss, err := s.f.scanSegments()
	if err != nil {
		return err
	}
	if len(ss) == 0 {
		if s.seq == 0 {
			return nil
		}
		return followError(CodeSegmentDeleted, "active/cursor segment disappeared with no replacement segment", s.seq, s.offset, nil)
	}
	if s.seq == 0 {
		if ss[0].seq != 1 {
			return followError(CodeSegmentGap,
				fmt.Sprintf("oldest segment is %04d but segment-0001 is missing; cannot start from the middle", ss[0].seq),
				ss[0].seq, 0, nil)
		}
		s.seq = ss[0].seq
		s.offset = 0
		s.known = initialKnown(ss)
	}
	highest := ss[len(ss)-1].seq
	if _, ok := segmentBySeq(ss, s.seq); !ok {
		// The segment the cursor currently points at disappeared. Whether the
		// sequence was skipped or later filled, evidence at this point is gone;
		// never silently jump to the newest file.
		return followError(CodeSegmentDeleted, "current segment disappeared", s.seq, s.offset, nil)
	}
	for seq := s.seq + 1; seq <= highest; seq++ {
		if _, ok := segmentBySeq(ss, seq); !ok {
			return followError(CodeSegmentGap,
				fmt.Sprintf("segment %04d is missing before highest segment %04d", seq, highest),
				s.seq, s.offset, nil)
		}
	}

	if err := s.checkKnown(ss, highest); err != nil {
		return err
	}

	// Process one or more already sealed segments, but never concatenate the
	// tail of one with the head of the next.
	for s.seq < highest {
		cur, _ := segmentBySeq(ss, s.seq)
		if err := s.readToSealedEnd(cur); err != nil {
			return err
		}
		nextSeq := s.seq + 1
		if _, ok := segmentBySeq(ss, nextSeq); !ok {
			return followError(CodeSegmentGap, "next segment disappeared before advancing", nextSeq, 0, nil)
		}
		s.seq = nextSeq
		s.offset = 0
		s.pending = nil
	}

	current, _ := segmentBySeq(ss, s.seq)
	return s.readActive(current)
}

func (s *followState) checkKnown(ss []segmentInfo, highest int) error {
	for seq, old := range s.known {
		if seq < s.seq {
			continue
		}
		info, ok := segmentBySeq(ss, seq)
		if !ok {
			if seq >= s.seq && seq <= highest {
				return followError(CodeSegmentDeleted, fmt.Sprintf("segment %04d disappeared", seq), seq, 0, nil)
			}
			continue
		}
		if info.id != old.id {
			return followError(CodeSegmentReplaced,
				fmt.Sprintf("segment %04d was replaced by a different file (inode/device changed); copy/replace rotation is not handled", seq),
				seq, s.currentOffset(seq), nil)
		}
		if seq < highest && info.size < old.size {
			return followError(CodeSealedModified,
				fmt.Sprintf("sealed segment %04d shrank from %d bytes to %d bytes", seq, old.size, info.size),
				seq, info.size, nil)
		}
	}
	for _, info := range ss {
		if _, exists := s.known[info.seq]; !exists {
			s.known[info.seq] = knownSegment{id: info.id, size: info.size}
		}
	}
	return nil
}

func (s *followState) currentOffset(seq int) int64 {
	if seq == s.seq {
		return s.offset
	}
	return 0
}

func (s *followState) openChecked(info segmentInfo) (*os.File, os.FileInfo, error) {
	file, err := os.Open(info.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, followError(CodeSegmentDeleted, "segment disappeared before open", info.seq, s.currentOffset(info.seq), err)
		}
		return nil, nil, followError(CodeReadFailed, "cannot open segment", info.seq, s.currentOffset(info.seq), err)
	}
	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, followError(CodeReadFailed, "cannot stat open segment", info.seq, s.currentOffset(info.seq), err)
	}
	id, err := fileIdentityFromInfo(st)
	if err != nil {
		file.Close()
		return nil, nil, followError(CodeReadFailed, "cannot identify open segment", info.seq, s.currentOffset(info.seq), err)
	}
	// The active segment may legitimately have grown after the directory
	// scan, so only identity is compared here. Shrinking is checked by
	// callers against the previously observed size.
	if id != info.id {
		file.Close()
		return nil, nil, followError(CodeSegmentReplaced, "segment inode changed between directory scan and open", info.seq, s.currentOffset(info.seq), nil)
	}
	return file, st, nil
}

func (s *followState) readToSealedEnd(info segmentInfo) error {
	old := s.known[info.seq]
	// The active file became sealed because a higher-numbered segment appeared.
	// It may have gained its final newline/bytes during that transition, but it
	// must never shrink.
	if info.size < old.size {
		return followError(CodeSealedModified,
			fmt.Sprintf("sealed segment shrank from %d bytes to %d bytes", old.size, info.size),
			info.seq, info.size, nil)
	}
	if err := s.readAvailable(info, false); err != nil {
		return err
	}
	if len(s.pending) > 0 || s.offset != info.size {
		return followError(CodeSealedTruncated,
			"sealed segment ends without a newline; refusing to join its partial line to the next segment",
			info.seq, s.offset, nil)
	}
	ok, err := endsWithNewlineAt(info.path, info.size)
	if err != nil {
		return followError(CodeReadFailed, "cannot revalidate sealed segment boundary", info.seq, s.offset, err)
	}
	if !ok {
		return followError(CodeSealedTruncated, "sealed segment does not end with a newline", info.seq, s.offset, nil)
	}
	s.known[info.seq] = knownSegment{id: info.id, size: info.size}
	return nil
}

func (s *followState) readActive(info segmentInfo) error {
	old := s.known[info.seq]
	if info.size < s.offset || info.size < old.size {
		return followError(CodeSegmentTruncated,
			fmt.Sprintf("active segment shrank from %d bytes to %d bytes", old.size, info.size),
			info.seq, info.size, nil)
	}
	if err := s.readAvailable(info, true); err != nil {
		return err
	}
	// The active segment is allowed to grow. Update its observed size for the
	// next shrink/replacement check; device/inode remains immutable.
	s.known[info.seq] = knownSegment{id: info.id, size: s.offset}
	return nil
}

func (s *followState) readAvailable(info segmentInfo, allowGrowth bool) error {
	file, st, err := s.openChecked(info)
	if err != nil {
		return err
	}
	defer file.Close()

	size := st.Size()
	if !allowGrowth {
		// A sealed segment is immutable; growth after sealing is evidence of
		// unsupported copy/replace-style tampering, not normal appending.
		if size != info.size {
			return followError(CodeSealedModified,
				fmt.Sprintf("sealed segment changed size from %d to %d after directory scan", info.size, size),
				info.seq, size, nil)
		}
	}
	if size > MaxSegmentSize {
		return followError(CodeSegmentOversized,
			fmt.Sprintf("segment is %d bytes; hard limit is %d", size, MaxSegmentSize),
			info.seq, size, nil)
	}
	if size < s.offset {
		return followError(CodeSegmentTruncated, "segment shrank before read", info.seq, size, nil)
	}
	// Segment files are bounded at 8 KiB; one physical read covers all bytes
	// currently available without a character decoder or fixed-line limit.
	buf := make([]byte, size-s.offset)
	if len(buf) > 0 {
		n, err := file.ReadAt(buf, s.offset)
		if n != len(buf) {
			cause := err
			if cause == nil {
				cause = errors.New("short read")
			}
			return followError(CodeReadFailed, "short read from segment", info.seq, s.offset, cause)
		}
		s.pending = append(s.pending, buf[:n]...)
		s.offset += int64(n)
	}
	return s.emitCompleteLines(info.seq)
}

func (s *followState) emitCompleteLines(seq int) error {
	for {
		idx := bytes.IndexByte(s.pending, '\n')
		if idx < 0 {
			if len(s.pending) > MaxSegmentSize {
				return followError(CodeSegmentOversized, "pending line exceeds segment size limit", seq, s.offset, nil)
			}
			return nil
		}
		line := s.pending[:idx]
		line = bytes.TrimSuffix(line, []byte{'\r'})
		base := s.offset - int64(len(s.pending))
		end := base + int64(idx) + 1
		if err := s.emitLine(seq, end, line); err != nil {
			return err
		}
		s.pending = s.pending[idx+1:]
	}
}

func (s *followState) emitLine(seq int, end int64, line []byte) error {
	ev := Event{
		Type:      "record",
		RunID:     s.f.runID,
		Segment:   seq,
		EndOffset: end,
	}
	// A complete line is parsed only at a real byte newline. RFC 8259 requires
	// JSON text to be UTF-8; encoding/json itself tolerates invalid UTF-8 inside
	// strings, so validate it explicitly.
	if utf8.Valid(line) && json.Valid(line) {
		// Copy: json.RawMessage escapes into the SSE data payload rather than
		// retaining the follower's reusable read buffer.
		ev.Result = append(json.RawMessage(nil), line...)
	} else {
		ev.Type = "invalid_record"
		ev.Raw = append([]byte(nil), line...)
		reason := "complete newline-delimited line is not valid JSON"
		if !utf8.Valid(line) {
			reason = "complete newline-delimited line contains invalid UTF-8 and cannot be parsed as JSON"
		}
		ev.Error = &ErrorDetail{Code: "invalid_json", Message: reason}
	}
	// Never block a poller behind a slow consumer. Other subscriptions have
	// their own goroutines and bounded queues and are unaffected.
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sub.events <- ev:
		return nil
	default:
		return followError(CodeSlowConsumer,
			"subscriber event buffer is full; connection is being terminated to protect other subscribers",
			seq, end, nil)
	}
}
