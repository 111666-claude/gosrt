package srt

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// StatsSubscriptionMaxBuffer is the largest number of snapshots a
// StatsSubscription can hold for a slow consumer. Buffers larger than this
// are rejected so that a forgotten subscription cannot grow without bound.
const StatsSubscriptionMaxBuffer = 1024

// StatsSubscriptionMinInterval is the shortest sampling interval a
// StatsSubscription accepts. The interval statistics are expressed in
// milliseconds, so a shorter interval could not produce meaningful rates.
const StatsSubscriptionMinInterval = time.Millisecond

var (
	// ErrInvalidStatsInterval is returned by Conn.SubscribeStats when the
	// sampling interval is shorter than StatsSubscriptionMinInterval, in
	// particular when it is zero or negative.
	ErrInvalidStatsInterval = errors.New("srt: statistics subscription interval must be at least 1ms")

	// ErrInvalidStatsBuffer is returned by Conn.SubscribeStats when the
	// buffer size is outside of [1, StatsSubscriptionMaxBuffer].
	ErrInvalidStatsBuffer = errors.New("srt: statistics subscription buffer is out of range")
)

// StatsSubscriptionConfig configures a periodic statistics subscription.
type StatsSubscriptionConfig struct {
	// Interval is the time between two samples. It must be at least
	// StatsSubscriptionMinInterval (1ms); shorter intervals, in particular
	// zero and negative ones, are rejected with ErrInvalidStatsInterval.
	Interval time.Duration

	// Buffer is the maximum number of snapshots that may be queued for a
	// consumer that is not reading fast enough. It must be in the range
	// [1, StatsSubscriptionMaxBuffer]. When the buffer is full, the oldest
	// queued snapshot is discarded to make room for the newest one, so a
	// slow consumer always observes the most recent sampling.
	Buffer int
}

// StatsSnapshot is a single sample of the statistics of a connection.
type StatsSnapshot struct {
	// Seq is the 1-based number of this sample. A gap between the Seq of two
	// received snapshots means that the samples in between were dropped
	// because the consumer did not keep up with the buffer.
	Seq uint64

	// SampledAt is the wall-clock time at which the sample was taken. The
	// Statistics.MsTimeStamp field is relative to the creation of the
	// connection instead.
	SampledAt time.Time

	// Stats contains the accumulated, interval and instantaneous statistics
	// at SampledAt. Interval values are the deltas since the previous sample
	// of this subscription; for the first snapshot they cover the time since
	// the subscription was started.
	Stats Statistics
}

// StatsSubscription is a periodic statistics subscription created by
// Conn.SubscribeStats. Multiple subscriptions of the same connection are
// independent: each has its own sampling interval, buffer and interval
// counters.
//
// A subscription must be cancelled by its owner. It also terminates when the
// connection is closed.
type StatsSubscription interface {
	// C returns the channel on which snapshots are delivered. The channel is
	// closed when the subscription is cancelled or the connection is closed.
	// It is never closed while a snapshot is in flight, and a receive from a
	// closed channel therefore always succeeds with ok == false.
	C() <-chan StatsSnapshot

	// Dropped returns the number of snapshots that were discarded because
	// the consumer did not drain the buffer fast enough. Snapshots are
	// dropped oldest-first, so the consumer always sees the newest sample.
	Dropped() uint64

	// Cancel stops the sampling timer, closes C and releases the resources of
	// the subscription. Snapshots that were produced but not consumed yet are
	// discarded, so a receive from C after Cancel returns always reports a
	// closed channel. Cancel is idempotent and safe to call concurrently.
	// When Cancel returns, no further snapshots will be delivered and C is
	// closed. Cancelling a subscription of an already closed connection is a
	// no-op.
	Cancel()
}

// statsSubscription owns the sampling goroutine and the output channel. Only
// that goroutine writes to out and only that goroutine closes it, which makes
// Cancel and connection close free of send-on-closed-channel races.
type statsSubscription struct {
	conn     *srtConn
	interval time.Duration
	out      chan StatsSnapshot
	cancelCh chan struct{}
	done     chan struct{}
	cancel   sync.Once

	seq     uint64
	dropped atomic.Uint64

	// stats is only accessed by the sampler goroutine (and by
	// newStatsSubscription before the goroutine starts).
	stats *Statistics
}

// newStatsSubscription validates the configuration and starts the sampler. A
// subscription for a nil or already closed connection is returned already
// terminated, with C closed and Cancel being a no-op, so that subscribing
// while a connection is closing never hangs or leaks a goroutine.
func newStatsSubscription(c *srtConn, config StatsSubscriptionConfig) (*statsSubscription, error) {
	if config.Interval < StatsSubscriptionMinInterval {
		return nil, ErrInvalidStatsInterval
	}

	if config.Buffer < 1 || config.Buffer > StatsSubscriptionMaxBuffer {
		return nil, ErrInvalidStatsBuffer
	}

	s := &statsSubscription{
		conn:     c,
		interval: config.Interval,
		out:      make(chan StatsSnapshot, config.Buffer),
		cancelCh: make(chan struct{}),
		done:     make(chan struct{}),
		stats:    &Statistics{},
	}

	if c == nil {
		close(s.out)
		close(s.done)
		return s, nil
	}

	if !c.addStatsSubscription(s) {
		close(s.out)
		close(s.done)
		return s, nil
	}

	// Prime the statistics so that the first delivered snapshot reports the
	// interval since the subscription started rather than since the
	// connection was created. This happens synchronously, before the sampler
	// goroutine starts, so that the first interval is measured from the
	// moment SubscribeStats returned.
	c.Stats(s.stats)

	go s.run()

	return s, nil
}

// addStatsSubscription registers a subscription with the connection. It
// reports false if the connection has already been shut down, in which case
// the subscription must not start.
func (c *srtConn) addStatsSubscription(s *statsSubscription) bool {
	c.statsSubscriptionsLock.Lock()
	defer c.statsSubscriptionsLock.Unlock()

	if c.ctx == nil || c.ctx.Err() != nil {
		return false
	}

	if c.statsSubscriptions == nil {
		c.statsSubscriptions = make(map[*statsSubscription]struct{})
	}

	c.statsSubscriptions[s] = struct{}{}

	return true
}

// stopStatsSubscriptions asks every live subscription to terminate. The
// connection context is already cancelled at that point, so the subscription
// goroutines stop on their own; this only drops the references they hold.
func (c *srtConn) stopStatsSubscriptions() {
	c.statsSubscriptionsLock.Lock()
	subscriptions := c.statsSubscriptions
	c.statsSubscriptions = nil
	c.statsSubscriptionsLock.Unlock()

	for s := range subscriptions {
		s.stop()
	}
}

// removeStatsSubscription forgets a subscription that has been cancelled.
func (c *srtConn) removeStatsSubscription(s *statsSubscription) {
	c.statsSubscriptionsLock.Lock()
	delete(c.statsSubscriptions, s)
	c.statsSubscriptionsLock.Unlock()
}

func (s *statsSubscription) C() <-chan StatsSnapshot {
	return s.out
}

func (s *statsSubscription) Dropped() uint64 {
	return s.dropped.Load()
}

// stop tells the sampler to terminate without waiting for it. The sampler
// closes the output channel when it exits.
func (s *statsSubscription) stop() {
	s.cancel.Do(func() {
		close(s.cancelCh)
	})
}

func (s *statsSubscription) Cancel() {
	s.stop()

	<-s.done

	if s.conn != nil {
		s.conn.removeStatsSubscription(s)
	}
}

func (s *statsSubscription) run() {
	defer close(s.done)
	defer func() {
		// Discard snapshots that were produced but not consumed, so that a
		// receive after termination (or after Cancel returned) never yields a
		// stale sample. Only the sampler sends on or closes s.out, so draining
		// here cannot race with another sender.
		for {
			select {
			case <-s.out:
			default:
				close(s.out)
				return
			}
		}
	}()

	stats := s.stats

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.conn.ctx.Done():
			return
		case <-s.cancelCh:
			return
		case <-ticker.C:
		}

		// If the tick raced with cancellation or connection shutdown, prefer
		// termination over emitting one more sample.
		select {
		case <-s.conn.ctx.Done():
			return
		case <-s.cancelCh:
			return
		default:
		}

		sampledAt := time.Now()
		s.conn.Stats(stats)
		s.seq++
		s.offer(StatsSnapshot{
			Seq:       s.seq,
			SampledAt: sampledAt,
			Stats:     *stats,
		})
	}
}

// offer hands a snapshot to the consumer without ever blocking the sampling
// goroutine. If the bounded buffer is full, the oldest queued snapshot is
// dropped in favour of snap, so a slow consumer falls behind by at most the
// configured buffer and always has the newest sample available.
func (s *statsSubscription) offer(snap StatsSnapshot) {
	select {
	case s.out <- snap:
		return
	default:
	}

	// The buffer was full a moment ago. Make room by dropping the oldest
	// snapshot; the consumer may have raced us and already taken it, which is
	// fine. Only this goroutine sends on or receives from s.out besides the
	// consumer, so no other snapshot can have appeared in between.
	select {
	case <-s.out:
		s.dropped.Add(1)
	default:
	}

	select {
	case s.out <- snap:
	default:
		// A consumer that is ready to receive may have taken the fresh
		// snapshot's slot; dropping this sample is still bounded by Buffer.
		s.dropped.Add(1)
	}
}

// SubscribeStats starts a periodic statistics subscription. The subscription
// samples the connection on its own goroutine; it never runs on the send or
// receive path, and a slow consumer therefore cannot slow down the connection.
//
// Each subscription maintains its own interval counters. The first snapshot is
// delivered one Interval after SubscribeStats returns and its Interval values
// cover the time since the subscription started. Later snapshots report the
// deltas since the previous sample of the same subscription.
//
// A subscription must be cancelled by its owner unless the connection is
// closed first. Cancel never blocks on a consumer.
//
// If the connection is already closed, the returned subscription is already
// terminated: C is closed and Cancel is a no-op.
func (c *srtConn) SubscribeStats(config StatsSubscriptionConfig) (StatsSubscription, error) {
	return newStatsSubscription(c, config)
}
