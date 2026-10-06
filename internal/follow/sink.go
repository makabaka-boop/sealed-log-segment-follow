package follow

import (
	"context"
	"errors"
	"time"

	"logfollow/internal/logdir"
)

// ErrSlowConsumer is returned by Emit when the subscriber's bounded buffer
// has stayed full longer than the configured grace period.
var ErrSlowConsumer = errors.New("follow: subscriber buffer full")

// Sink receives delivered events. Implementations must be safe for one
// concurrent emitter per subscription.
type Sink interface {
	Emit(ctx context.Context, ev logdir.Event) error
}

// ChanSink is a capacity-bounded event queue. The follower blocks for at most
// FullWait on a full queue, then abandons the subscription with a documented
// slow-consumer reason instead of growing memory indefinitely or blocking the
// process (other subscriptions have their own independent queues).
type ChanSink struct {
	ch       chan logdir.Event
	fullWait time.Duration
}

// NewChanSink builds a sink with the given queue capacity and grace window.
func NewChanSink(buffer int, fullWait time.Duration) *ChanSink {
	return &ChanSink{
		ch:       make(chan logdir.Event, buffer),
		fullWait: fullWait,
	}
}

// Events returns the receive side of the event channel. It is closed by Close.
func (s *ChanSink) Events() <-chan logdir.Event { return s.ch }

// Emit enqueues one event. A context cancellation ends the attempt quietly;
// a persistently full queue returns ErrSlowConsumer.
func (s *ChanSink) Emit(ctx context.Context, ev logdir.Event) error {
	select {
	case s.ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	t := time.NewTimer(s.fullWait)
	defer t.Stop()
	select {
	case s.ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrSlowConsumer
	}
}

// Close releases the channel. It is safe to call once the follower has
// stopped; the SSE writer drains the channel first.
func (s *ChanSink) Close() { close(s.ch) }
