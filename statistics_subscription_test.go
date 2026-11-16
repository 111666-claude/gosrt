package srt

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

// startStatsFixture returns a connection whose goroutines are running but
// which is not attached to a network socket. It is enough for the statistics
// sampler: Stats only reads the congestion control state and the connection
// counters.
func startStatsFixture(t *testing.T) *srtConn {
	t.Helper()

	cfg := DefaultConfig()
	// The fixture is not reachable from any peer; keep the idle timeout from
	// racing the test's own close.
	cfg.PeerIdleTimeout = time.Minute

	c := newSRTConn(srtConnConfig{
		version:      5,
		isCaller:     true,
		localAddr:    mustAddr("127.0.0.1:1000"),
		remoteAddr:   mustAddr("127.0.0.1:2000"),
		config:       cfg,
		start:        time.Now(),
		socketId:     1,
		peerSocketId: 2,
		logger:       NewLogger(nil),
	})
	t.Cleanup(c.close)

	return c
}

// addSentPackets pushes n packets into the sender and releases them, so that
// the accumulated statistics counters advance deterministically.
func addSentPackets(c *srtConn, n int) {
	for range n {
		p := packet.NewPacket(c.remoteAddr)
		p.Header().PktTsbpdTime = c.getTimestamp() - 1
		p.SetData(make([]byte, 1316))

		c.snd.Push(p)
	}

	c.snd.Tick(c.getTimestamp())
}

func TestSubscribeStatsRejectsInvalidConfig(t *testing.T) {
	c := startStatsFixture(t)

	for _, interval := range []time.Duration{0, -time.Second, time.Microsecond} {
		sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: interval, Buffer: 1})
		require.ErrorIs(t, err, ErrInvalidStatsInterval, "interval %s", interval)
		require.Nil(t, sub)
	}

	for _, buffer := range []int{-1, 0, StatsSubscriptionMaxBuffer + 1} {
		sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: time.Second, Buffer: buffer})
		require.ErrorIs(t, err, ErrInvalidStatsBuffer, "buffer %d", buffer)
		require.Nil(t, sub)
	}
}

func TestSubscribeStatsDeliversCumulativeAndIntervalStats(t *testing.T) {
	c := startStatsFixture(t)

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 20 * time.Millisecond, Buffer: 8})
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	addSentPackets(c, 10)

	first := receiveSnapshot(t, sub, 2*time.Second)
	require.Equal(t, uint64(1), first.Seq)
	require.False(t, first.SampledAt.IsZero())
	require.GreaterOrEqual(t, first.Stats.Accumulated.PktSent, uint64(10))
	require.Equal(t, first.Stats.Accumulated.PktSent, first.Stats.Interval.PktSent,
		"the first snapshot covers the time since the subscription started")
	// Instantaneous values such as the smoothed RTT are included as well.
	require.Positive(t, first.Stats.Instantaneous.MsRTT)

	before := first.Stats.Accumulated.PktSent
	addSentPackets(c, 5)

	second := receiveSnapshot(t, sub, 2*time.Second)
	require.Equal(t, uint64(2), second.Seq)
	require.GreaterOrEqual(t, second.Stats.Accumulated.PktSent, before+5)
	require.Equal(t, second.Stats.Accumulated.PktSent-before, second.Stats.Interval.PktSent)
	require.GreaterOrEqual(t, second.Stats.Interval.ByteSent, second.Stats.Interval.PktSent*1316)
	require.True(t, second.SampledAt.After(first.SampledAt))
}

func TestStatsSubscriptionsAreIndependent(t *testing.T) {
	c := startStatsFixture(t)

	slow, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 500 * time.Millisecond, Buffer: 4})
	require.NoError(t, err)
	t.Cleanup(slow.Cancel)

	fast, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 10 * time.Millisecond, Buffer: 4})
	require.NoError(t, err)
	t.Cleanup(fast.Cancel)

	addSentPackets(c, 7)

	// The fast subscription takes its samples first and must still report the
	// packets as interval traffic in its first snapshot.
	fastFirst := receiveSnapshot(t, fast, 2*time.Second)
	require.Equal(t, uint64(1), fastFirst.Seq)
	require.Equal(t, fastFirst.Stats.Accumulated.PktSent, fastFirst.Stats.Interval.PktSent)
	receiveSnapshot(t, fast, 2*time.Second)

	// The slow subscription has its own interval bookkeeping: the samples of
	// the fast one neither consume nor reset it, so its first snapshot still
	// reports all packets since it was started.
	slowFirst := receiveSnapshot(t, slow, 2*time.Second)
	require.Equal(t, uint64(1), slowFirst.Seq)
	require.Equal(t, slowFirst.Stats.Accumulated.PktSent, slowFirst.Stats.Interval.PktSent)

	// Two more samples of the fast subscription must not affect the slow one.
	receiveSnapshot(t, fast, 2*time.Second)
	receiveSnapshot(t, fast, 2*time.Second)

	addSentPackets(c, 3)
	slowSecond := receiveSnapshot(t, slow, 2*time.Second)
	require.Equal(t, uint64(2), slowSecond.Seq)
	require.Equal(t, slowSecond.Stats.Accumulated.PktSent-slowFirst.Stats.Accumulated.PktSent, slowSecond.Stats.Interval.PktSent)
}

func TestSubscribeStatsDropsOldestOnBackpressure(t *testing.T) {
	c := startStatsFixture(t)

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 5 * time.Millisecond, Buffer: 2})
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	// Do not read for long enough that many more samples than the buffer size
	// are produced. The sampler must not block on the full buffer.
	time.Sleep(200 * time.Millisecond)

	require.Positive(t, sub.Dropped())

	first := receiveSnapshot(t, sub, time.Second)
	second := receiveSnapshot(t, sub, time.Second)

	// The buffer holds the newest samples, so the sequence numbers are near the
	// total number of samples taken and consecutive. Dropped only grows over
	// time, so it must be at least the number of samples skipped before the
	// oldest snapshot that was kept.
	require.Greater(t, first.Seq, uint64(2))
	require.Equal(t, first.Seq+1, second.Seq)
	require.GreaterOrEqual(t, sub.Dropped(), first.Seq-1)

	// After the consumer catches up, sampling continues from the last seen
	// sample.
	third := receiveSnapshot(t, sub, time.Second)
	require.GreaterOrEqual(t, third.Seq, second.Seq+1)
}

func TestSubscribeStatsCancelStopsSamplingAndClosesChannel(t *testing.T) {
	c := startStatsFixture(t)

	// Let the connection's own goroutines start before taking the baseline, so
	// the comparison below only accounts for the subscription itself.
	time.Sleep(20 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 5 * time.Millisecond, Buffer: 4})
	require.NoError(t, err)

	receiveSnapshot(t, sub, time.Second)

	sub.Cancel()
	sub.Cancel() // repeated cancel is a no-op

	_, ok := <-sub.C()
	require.False(t, ok, "the channel must be closed after Cancel")

	// No goroutine may remain behind for this subscription.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), baseline)
}

func TestSubscribeStatsConnectionCloseTerminates(t *testing.T) {
	c := startStatsFixture(t)

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 5 * time.Millisecond, Buffer: 4})
	require.NoError(t, err)

	receiveSnapshot(t, sub, time.Second)
	c.close()

	select {
	case _, ok := <-sub.C():
		require.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed after the connection closed")
	}

	// Cancelling after connection close must neither block nor panic.
	sub.Cancel()
}

func TestSubscribeStatsOnClosedConnectionIsNoop(t *testing.T) {
	c := startStatsFixture(t)
	c.close()

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: time.Second, Buffer: 1})
	require.NoError(t, err)
	require.NotNil(t, sub)

	select {
	case _, ok := <-sub.C():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel of a closed connection must be closed")
	}

	sub.Cancel()
}

func TestSubscribeStatsCancelDuringClose(t *testing.T) {
	for range 50 {
		c := startStatsFixture(t)
		sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: time.Millisecond, Buffer: 1})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			sub.Cancel()
			close(done)
		}()

		c.close()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Cancel blocked while the connection was closing")
		}
	}
}

func TestSubscribeStatsOverLiveConnection(t *testing.T) {
	cfg := DefaultConfig()
	client, server := idleTestPair(t, cfg)

	sub, err := client.SubscribeStats(StatsSubscriptionConfig{Interval: 20 * time.Millisecond, Buffer: 16})
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	// Drain the server side so the write does not back up.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buffer := make([]byte, 2048)
		for {
			if _, err := server.Read(buffer); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		server.Close()
		<-readDone
	})

	first := receiveSnapshot(t, sub, 2*time.Second)

	payload := make([]byte, 1316)
	for range 20 {
		_, err := client.Write(payload)
		require.NoError(t, err)
	}

	var last StatsSnapshot
	deadline := time.Now().Add(2 * time.Second)
	for last.Stats.Interval.PktSent == 0 && time.Now().Before(deadline) {
		last = receiveSnapshot(t, sub, 2*time.Second)
	}

	require.Positive(t, last.Stats.Interval.PktSent)
	require.Greater(t, last.Stats.Accumulated.PktSent, first.Stats.Accumulated.PktSent)
	require.GreaterOrEqual(t, last.Stats.Accumulated.PktSent-first.Stats.Accumulated.PktSent, uint64(20))
	require.GreaterOrEqual(t, last.Stats.Interval.ByteSent, last.Stats.Interval.PktSent*1316)
	require.Greater(t, last.Seq, first.Seq)
}

func TestSubscribeStatsOnDialerWithoutConnection(t *testing.T) {
	dl := &dialer{}

	// Before the handshake completed there is nothing to sample. Mirroring
	// Stats, this is not an error: the subscription is already terminated.
	sub, err := dl.SubscribeStats(StatsSubscriptionConfig{Interval: time.Second, Buffer: 1})
	require.NoError(t, err)

	select {
	case _, ok := <-sub.C():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("channel must be closed for a dialer without a connection")
	}

	sub.Cancel()

	// Invalid configuration is still reported.
	_, err = dl.SubscribeStats(StatsSubscriptionConfig{Interval: 0, Buffer: 1})
	require.ErrorIs(t, err, ErrInvalidStatsInterval)
}

func TestSubscribeStatsConcurrentCancel(t *testing.T) {
	c := startStatsFixture(t)

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: time.Millisecond, Buffer: 2})
	require.NoError(t, err)

	done := make(chan struct{})
	for range 8 {
		go func() {
			sub.Cancel()
			done <- struct{}{}
		}()
	}

	for range 8 {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("a concurrent Cancel blocked")
		}
	}

	_, ok := <-sub.C()
	require.False(t, ok)
}

func TestSubscribeStatsCancelDiscardsBufferedSnapshots(t *testing.T) {
	c := startStatsFixture(t)

	sub, err := c.SubscribeStats(StatsSubscriptionConfig{Interval: 5 * time.Millisecond, Buffer: 8})
	require.NoError(t, err)

	// Let the buffer fill up, then cancel without draining it.
	time.Sleep(100 * time.Millisecond)
	sub.Cancel()

	select {
	case _, ok := <-sub.C():
		require.False(t, ok, "buffered snapshots must be discarded on Cancel")
	default:
		t.Fatal("C is neither closed nor readable after Cancel")
	}
}

func receiveSnapshot(t *testing.T, sub StatsSubscription, timeout time.Duration) StatsSnapshot {
	t.Helper()

	select {
	case snapshot, ok := <-sub.C():
		require.True(t, ok, "subscription channel closed unexpectedly")
		return snapshot
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a statistics snapshot")
		return StatsSnapshot{}
	}
}

func mustAddr(s string) net.Addr {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		panic(err)
	}

	return addr
}
