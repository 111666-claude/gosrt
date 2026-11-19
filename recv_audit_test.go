package srt

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

func TestRecvDeadlineStatusString(t *testing.T) {
	require.Equal(t, "ontime", RecvDeadlineOnTime.String())
	require.Equal(t, "late", RecvDeadlineLate.String())
	require.Equal(t, "dropped", RecvDeadlineDropped.String())
	require.Equal(t, "unknown", RecvDeadlineStatus(42).String())
}

func auditSeq(n uint32) circular.Number {
	return circular.New(n, packet.MAX_SEQUENCENUMBER)
}

// drainAuditEvents reads all currently queued events without blocking.
func drainAuditEvents(a *recvAudit) []RecvDeadlineEvent {
	var events []RecvDeadlineEvent

	for {
		select {
		case ev, ok := <-a.events:
			if !ok {
				return events
			}
			events = append(events, ev)
		default:
			return events
		}
	}
}

func TestRecvAuditDeliverOnTimeAndLate(t *testing.T) {
	a := newRecvAudit(16, 1024)
	defer a.close()

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	// Packet 1 arrives before its deadline, packet 2 after its deadline.
	a.register(auditSeq(1), 1000, deadline, now)
	a.register(auditSeq(2), 2000, deadline, deadline.Add(time.Millisecond))

	// Retransmissions and duplicates must not register again.
	a.register(auditSeq(1), 1000, deadline, now)
	a.register(auditSeq(2), 2000, deadline, deadline.Add(2*time.Millisecond))

	a.deliver(auditSeq(1), true, deadline)
	a.deliver(auditSeq(2), true, deadline.Add(2*time.Millisecond))

	events := drainAuditEvents(a)
	require.Len(t, events, 2)

	require.Equal(t, uint32(1), events[0].SequenceNumber)
	require.Equal(t, uint32(1000), events[0].Timestamp)
	require.Equal(t, RecvDeadlineOnTime, events[0].Status)
	require.Equal(t, deadline, events[0].Deadline)
	require.False(t, events[0].DeliveredAt.IsZero())

	require.Equal(t, uint32(2), events[1].SequenceNumber)
	require.Equal(t, RecvDeadlineLate, events[1].Status)
	require.False(t, events[1].DeliveredAt.IsZero())

	// A retransmission of an already reported packet must not produce an event.
	a.register(auditSeq(1), 1000, deadline, now)
	a.register(auditSeq(2), 2000, deadline, now)
	require.Empty(t, drainAuditEvents(a))
}

func TestRecvAuditBelatedAndSweep(t *testing.T) {
	a := newRecvAudit(16, 1024)
	defer a.close()

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	// 0 and 2 arrive, 1 is missing. 2 is buffered.
	a.register(auditSeq(0), 0, deadline, now)
	a.register(auditSeq(2), 2, deadline, now)

	a.deliver(auditSeq(0), true, deadline)

	// The receiver skips the gap once 2 becomes ripe.
	a.deliver(auditSeq(2), true, deadline.Add(2*time.Millisecond))

	// 1 was never registered, no event for it. A belated copy of 1 arriving
	// now is reported as dropped exactly once.
	a.register(auditSeq(1), 1, deadline, deadline.Add(200*time.Millisecond))
	a.register(auditSeq(1), 1, deadline, deadline.Add(201*time.Millisecond))

	events := drainAuditEvents(a)
	require.Len(t, events, 3)

	require.Equal(t, uint32(0), events[0].SequenceNumber)
	require.Equal(t, RecvDeadlineOnTime, events[0].Status)

	require.Equal(t, uint32(2), events[1].SequenceNumber)
	require.Equal(t, RecvDeadlineOnTime, events[1].Status)

	require.Equal(t, uint32(1), events[2].SequenceNumber)
	require.Equal(t, RecvDeadlineDropped, events[2].Status)
	require.True(t, events[2].DeliveredAt.IsZero())
}

func TestRecvAuditSweepRegisteredPacket(t *testing.T) {
	a := newRecvAudit(16, 1024)
	defer a.close()

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	// Both packets are registered, but the receiver delivers 1 while 0 is
	// still pending (e.g. after a DROPREQ). 0 must be reported as dropped.
	a.register(auditSeq(0), 0, deadline, now)
	a.register(auditSeq(1), 1, deadline, now)

	a.deliver(auditSeq(1), true, deadline)

	events := drainAuditEvents(a)
	require.Len(t, events, 2)

	require.Equal(t, uint32(1), events[0].SequenceNumber)
	require.Equal(t, RecvDeadlineOnTime, events[0].Status)

	require.Equal(t, uint32(0), events[1].SequenceNumber)
	require.Equal(t, RecvDeadlineDropped, events[1].Status)
}

func TestRecvAuditSlowConsumer(t *testing.T) {
	a := newRecvAudit(1, 1024)
	defer a.close()

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	// The queue holds one event, nobody reads it.
	for i := uint32(0); i < 8; i++ {
		a.register(auditSeq(i), i, deadline, now)
		a.deliver(auditSeq(i), true, deadline)
	}

	require.Equal(t, uint64(7), a.droppedEvents.Load())
	require.Len(t, drainAuditEvents(a), 1)
}

func TestRecvAuditClose(t *testing.T) {
	a := newRecvAudit(16, 1024)

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	a.register(auditSeq(0), 0, deadline, now)
	a.deliver(auditSeq(0), true, deadline)
	a.register(auditSeq(1), 1, deadline, now) // still pending at close

	a.close()

	// State is cleaned up, the channel is closed, buffered events survive.
	events := drainAuditEvents(a)
	require.Len(t, events, 1)
	require.Equal(t, uint32(0), events[0].SequenceNumber)
	require.Nil(t, a.pending)
	require.Nil(t, a.reported)

	// Closing again and any further activity is a no-op.
	a.close()
	a.register(auditSeq(2), 2, deadline, now)
	a.deliver(auditSeq(2), true, deadline)
}

func TestRecvAuditPrune(t *testing.T) {
	a := newRecvAudit(2048, 1024)
	defer a.close()

	now := time.Now()
	deadline := now.Add(100 * time.Millisecond)

	for i := uint32(0); i < 1100; i++ {
		a.register(auditSeq(i), i, deadline, now)
		a.deliver(auditSeq(i), true, deadline)
	}

	// The reported set is bounded by the prune window.
	require.LessOrEqual(t, len(a.reported), 1024+1)
}

// auditTestConn returns a connection with the receive deadline audit enabled
// and a TSBPD delay of 60ms. Packets are injected with handlePacket.
func auditTestConn(t *testing.T, capacity uint32) *srtConn {
	t.Helper()

	start := time.Now().Add(-500 * time.Millisecond)

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:6000")

	config := DefaultConfig()
	config.RecvDeadlineAudit = capacity

	c := newSRTConn(srtConnConfig{
		version:                     5,
		isCaller:                    true,
		localAddr:                   addr,
		remoteAddr:                  addr,
		config:                      config,
		start:                       start,
		socketId:                    1,
		peerSocketId:                2,
		tsbpdTimeBase:               uint64(time.Since(start).Microseconds()),
		tsbpdDelay:                  uint64((60 * time.Millisecond).Microseconds()),
		peerTsbpdDelay:              uint64((60 * time.Millisecond).Microseconds()),
		initialPacketSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER),
		onSend:                      func(p packet.Packet) {},
		onShutdown:                  func(c *srtConn) {},
		logger:                      NewLogger(nil),
	})
	t.Cleanup(func() { c.Close() })

	return c
}

// auditInject simulates the arrival of a data packet from a peer whose clock
// started at peerStart. sentAgo tells how long ago the packet was sent on the
// peer's clock, retrans marks it as a retransmission. It returns the media
// timestamp of the packet.
func auditInject(c *srtConn, peerStart time.Time, seq uint32, sentAgo time.Duration, retrans bool) uint32 {
	p := packet.NewPacket(c.localAddr)
	p.Header().PacketSequenceNumber = circular.New(seq, packet.MAX_SEQUENCENUMBER)
	p.Header().MessageNumber = 1
	p.Header().Timestamp = uint32(time.Since(peerStart.Add(sentAgo)).Microseconds())
	p.Header().RetransmittedPacketFlag = retrans
	p.SetData([]byte{byte(seq)})

	c.handlePacket(p)

	return p.Header().Timestamp
}

func nextAuditEvent(t *testing.T, ch <-chan RecvDeadlineEvent) RecvDeadlineEvent {
	t.Helper()

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "audit event channel closed unexpectedly")
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for audit event")
		return RecvDeadlineEvent{}
	}
}

func requireNoAuditEvent(t *testing.T, ch <-chan RecvDeadlineEvent, d time.Duration) {
	t.Helper()

	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("unexpected audit event: %+v", ev)
		}
		t.Fatal("audit event channel closed unexpectedly")
	case <-time.After(d):
	}
}

// TestRecvDeadlineAuditDelivery injects packets from a local fake peer with
// controlled send times and retransmissions and checks the events and the
// delivery of the original data.
func TestRecvDeadlineAuditDelivery(t *testing.T) {
	c := auditTestConn(t, 64)

	events := c.RecvDeadlineEvents()
	require.NotNil(t, events)

	// The peer clock started in the past so that packets sent "long ago"
	// still have a positive timestamp.
	peerStart := time.Now().Add(-time.Second)

	timestamps := map[uint32]uint32{}
	inject := func(seq uint32, sentAgo time.Duration, retrans bool) {
		timestamps[seq] = auditInject(c, peerStart, seq, sentAgo, retrans)
	}

	// In-order, just in time packets.
	inject(0, 0, false)
	inject(1, 0, false)
	inject(2, 0, false)

	// A packet that arrives 60ms after its deadline (sent 120ms ago with a
	// TSBPD delay of 60ms).
	inject(3, 120*time.Millisecond, false)

	// A gap at 4, the packets after it.
	inject(5, 0, false)
	inject(6, 0, false)

	for i := uint32(0); i <= 6; i++ {
		if i == 4 {
			continue
		}

		ev := nextAuditEvent(t, events)

		require.Equal(t, i, ev.SequenceNumber)
		require.Equal(t, timestamps[i], ev.Timestamp)
		require.False(t, ev.ArrivedAt.IsZero())

		if i == 3 {
			require.Equal(t, RecvDeadlineLate, ev.Status)
			require.True(t, ev.ArrivedAt.After(ev.Deadline), "late packet must arrive after its deadline")
		} else {
			require.Equal(t, RecvDeadlineOnTime, ev.Status)
			require.True(t, ev.ArrivedAt.Before(ev.Deadline), "on-time packet must arrive before its deadline")
			require.InDelta(t, (60 * time.Millisecond).Microseconds(), ev.Deadline.Sub(ev.ArrivedAt).Microseconds(), float64((20 * time.Millisecond).Microseconds()))
		}

		require.False(t, ev.DeliveredAt.IsZero())
	}

	// 4 arrives after the receiver moved past it: dropped, reported once.
	auditInject(c, peerStart, 4, 0, false)

	ev := nextAuditEvent(t, events)
	require.Equal(t, uint32(4), ev.SequenceNumber)
	require.Equal(t, RecvDeadlineDropped, ev.Status)
	require.True(t, ev.DeliveredAt.IsZero())

	// Retransmissions of already decided packets produce no events.
	auditInject(c, peerStart, 4, 0, true)
	auditInject(c, peerStart, 5, 0, true)
	auditInject(c, peerStart, 5, 0, false)
	requireNoAuditEvent(t, events, 150*time.Millisecond)

	// The original data is delivered in order, the lost packet skipped.
	want := []byte{0, 1, 2, 3, 5, 6}
	got := []byte{}

	for range want {
		p, err := c.ReadPacket()
		require.NoError(t, err)
		got = append(got, p.Data()...)
		p.Decommission()
	}

	require.Equal(t, want, got)
}

// TestRecvDeadlineAuditTimestampWrap checks that the deadlines around a wrap
// of the 32bit timestamp stay monotonic.
func TestRecvDeadlineAuditTimestampWrap(t *testing.T) {
	c := auditTestConn(t, 16)

	events := c.RecvDeadlineEvents()

	inject := func(seq uint32, timestamp uint32) {
		p := packet.NewPacket(c.localAddr)
		p.Header().PacketSequenceNumber = circular.New(seq, packet.MAX_SEQUENCENUMBER)
		p.Header().MessageNumber = 1
		p.Header().Timestamp = timestamp
		p.SetData([]byte{byte(seq)})

		c.handlePacket(p)
	}

	// 500us before the wrap of the timestamp, 1ms after the wrap.
	inject(0, packet.MAX_TIMESTAMP-500)
	time.Sleep(2 * time.Millisecond)
	inject(1, 500)

	ev0 := nextAuditEvent(t, events)
	ev1 := nextAuditEvent(t, events)

	require.Equal(t, RecvDeadlineOnTime, ev0.Status)
	require.Equal(t, RecvDeadlineOnTime, ev1.Status)

	// The deadline of the packet after the wrap is 1ms after the one before.
	require.InDelta(t, float64(time.Millisecond.Microseconds()), float64(ev1.Deadline.Sub(ev0.Deadline).Microseconds()), float64((10 * time.Millisecond).Microseconds()))
}

// TestRecvDeadlineAuditDisabled checks that a connection without the audit
// delivers data as usual and offers no event channel.
func TestRecvDeadlineAuditDisabled(t *testing.T) {
	start := time.Now().Add(-500 * time.Millisecond)

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:6000")

	c := newSRTConn(srtConnConfig{
		version:                     5,
		isCaller:                    true,
		localAddr:                   addr,
		remoteAddr:                  addr,
		config:                      DefaultConfig(),
		start:                       start,
		socketId:                    1,
		peerSocketId:                2,
		tsbpdTimeBase:               uint64(time.Since(start).Microseconds()),
		tsbpdDelay:                  uint64((60 * time.Millisecond).Microseconds()),
		peerTsbpdDelay:              uint64((60 * time.Millisecond).Microseconds()),
		initialPacketSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER),
		onSend:                      func(p packet.Packet) {},
		onShutdown:                  func(c *srtConn) {},
		logger:                      NewLogger(nil),
	})
	defer c.Close()

	require.Nil(t, c.RecvDeadlineEvents())
	require.Zero(t, c.RecvDeadlineEventsDropped())

	peerStart := time.Now()
	auditInject(c, peerStart, 0, 0, false)

	p, err := c.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{0}, p.Data())
	p.Decommission()
}

// TestRecvDeadlineAuditSlowConsumer checks that a full event queue neither
// blocks the connection nor loses track of the discards.
func TestRecvDeadlineAuditSlowConsumer(t *testing.T) {
	c := auditTestConn(t, 1)

	events := c.RecvDeadlineEvents()
	peerStart := time.Now()

	const n = 8

	for i := uint32(0); i < n; i++ {
		auditInject(c, peerStart, i, 0, false)
	}

	// Wait until all packets were delivered: the queue holds one event,
	// the rest was discarded.
	deadline := time.After(3 * time.Second)
	for c.RecvDeadlineEventsDropped() != n-1 {
		select {
		case <-deadline:
			t.Fatalf("packets were not delivered, dropped events: %d", c.RecvDeadlineEventsDropped())
		case <-time.After(10 * time.Millisecond):
		}
	}

	require.Equal(t, uint64(n-1), c.RecvDeadlineEventsDropped())

	ev := <-events
	require.Equal(t, uint32(0), ev.SequenceNumber)
}

// TestRecvDeadlineAuditClose checks that closing the connection closes the
// event channel and cleans up the audit state.
func TestRecvDeadlineAuditClose(t *testing.T) {
	c := auditTestConn(t, 16)

	events := c.RecvDeadlineEvents()
	peerStart := time.Now()

	auditInject(c, peerStart, 0, 0, false)

	ev := nextAuditEvent(t, events)
	require.Equal(t, uint32(0), ev.SequenceNumber)

	// 1 is still pending when the connection closes.
	auditInject(c, peerStart, 1, 0, false)

	c.Close()

	_, ok := <-events
	require.False(t, ok, "audit event channel must be closed with the connection")

	require.Nil(t, c.audit.pending)
	require.Nil(t, c.audit.reported)
}

// TestRecvDeadlineAuditEndToEnd sends data over a local pair of connections
// and checks the events against the delivered data.
func TestRecvDeadlineAuditEndToEnd(t *testing.T) {
	config := DefaultConfig()
	config.RecvDeadlineAudit = 64

	ln, err := Listen("srt", "127.0.0.1:0", config)
	require.NoError(t, err)
	t.Cleanup(ln.Close)

	accepted := make(chan Conn, 1)
	go func() {
		req, err := ln.Accept2()
		if err != nil {
			return
		}

		conn, err := req.Accept()
		if err != nil {
			return
		}

		accepted <- conn
	}()

	sender, err := Dial("srt", ln.Addr().String(), config)
	require.NoError(t, err)
	t.Cleanup(func() { sender.Close() })

	var receiver Conn
	select {
	case receiver = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("connection not accepted")
	}
	t.Cleanup(func() { receiver.Close() })

	events := receiver.RecvDeadlineEvents()
	require.NotNil(t, events)

	const n = 20

	// The sender controls the pacing of the packets.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)

		for i := 0; i < n; i++ {
			payload := bytes.Repeat([]byte{byte(i)}, 100)
			if _, err := sender.Write(payload); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Read the original data.
	readResult := make(chan []byte, 1)
	go func() {
		data := []byte{}
		buffer := make([]byte, 2048)

		for len(data) < n*100 {
			n, err := receiver.Read(buffer)
			if n > 0 {
				data = append(data, buffer[:n]...)
			}
			if err != nil {
				break
			}
		}

		readResult <- data
	}()

	// Collect one event per packet, in delivery order.
	collected := []RecvDeadlineEvent{}
	for len(collected) < n {
		select {
		case ev, ok := <-events:
			require.True(t, ok)
			collected = append(collected, ev)
		case <-time.After(5 * time.Second):
			t.Fatalf("got only %d of %d audit events", len(collected), n)
		}
	}

	<-sendDone

	var want []byte
	for i := 0; i < n; i++ {
		want = append(want, bytes.Repeat([]byte{byte(i)}, 100)...)
	}

	select {
	case got := <-readResult:
		require.Equal(t, want, got)
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive all data")
	}

	// On a loopback connection with 120ms latency every packet is on time.
	// The sequence numbers of the events are consecutive.
	for i, ev := range collected {
		require.Equal(t, RecvDeadlineOnTime, ev.Status, "event %d: %+v", i, ev)
		require.False(t, ev.DeliveredAt.IsZero())

		if i > 0 {
			prev := collected[i-1].SequenceNumber
			require.Equal(t, (prev+1)&packet.MAX_SEQUENCENUMBER, ev.SequenceNumber)
		}
	}

	require.Zero(t, receiver.RecvDeadlineEventsDropped())
}
