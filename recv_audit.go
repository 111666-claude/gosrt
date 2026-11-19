package srt

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
)

// RecvDeadlineStatus is the outcome of a received data packet with respect to
// its TSBPD delivery deadline.
type RecvDeadlineStatus int

const (
	// RecvDeadlineOnTime means the packet arrived before its scheduled
	// delivery time and was delivered to the application.
	RecvDeadlineOnTime RecvDeadlineStatus = iota

	// RecvDeadlineLate means the packet arrived after its scheduled delivery
	// time but could still be delivered to the application.
	RecvDeadlineLate

	// RecvDeadlineDropped means the packet was received but never delivered:
	// it arrived after the receiver had already moved past its sequence
	// number, or the connection was shut down before it could be delivered.
	RecvDeadlineDropped
)

func (s RecvDeadlineStatus) String() string {
	switch s {
	case RecvDeadlineOnTime:
		return "ontime"
	case RecvDeadlineLate:
		return "late"
	case RecvDeadlineDropped:
		return "dropped"
	}

	return "unknown"
}

// RecvDeadlineEvent reports how one received data packet fared against its
// TSBPD delivery deadline. Exactly one event is emitted per packet sequence
// number; retransmissions and reordered duplicates of an already seen packet
// do not produce another event.
//
// Packets that never arrive at all produce no event (their media timestamp
// and deadline are unknowable); they are visible as gaps in the sequence
// numbers of the events.
//
// Deadline, ArrivedAt, and DeliveredAt are taken from the monotonic clock,
// so comparing them is not affected by wall clock adjustments.
type RecvDeadlineEvent struct {
	// SequenceNumber is the packet sequence number.
	SequenceNumber uint32

	// Timestamp is the media timestamp of the packet in microseconds, as
	// sent by the peer. It wraps around as a 32bit value.
	Timestamp uint32

	// Deadline is the scheduled delivery time of the packet on the local
	// clock: the TSBPD time base plus the peer timestamp, the TSBPD delay
	// (receiver latency), and the measured peer clock offset and drift.
	Deadline time.Time

	// ArrivedAt is when the (first copy of the) packet arrived.
	ArrivedAt time.Time

	// DeliveredAt is when the packet was handed to the read queue. It is
	// the zero time when Status is RecvDeadlineDropped.
	DeliveredAt time.Time

	// Status is the outcome of the packet against its deadline.
	Status RecvDeadlineStatus
}

// recvAuditPending is the tracked state of a received packet that has not
// been delivered or dropped yet.
type recvAuditPending struct {
	timestamp uint32
	deadline  time.Time
	arrivedAt time.Time
	late      bool // arrived after its deadline
}

// recvAudit tracks the delivery deadline of every received data packet and
// emits exactly one event per sequence number on a bounded channel. It is
// safe for concurrent use; events are emitted while holding the lock, but
// the send on the channel never blocks.
type recvAudit struct {
	events chan RecvDeadlineEvent

	lock          sync.Mutex
	pending       map[uint32]recvAuditPending // received, awaiting the outcome
	reported      map[uint32]struct{}         // an event has been emitted
	delivered     circular.Number             // highest delivered sequence number
	haveDelivered bool
	pruneWindow   uint32 // how far behind the watermark reports are remembered
	closed        bool

	droppedEvents atomic.Uint64 // events discarded because the queue was full
}

// newRecvAudit returns a recvAudit with an event queue of the given capacity.
// Reports are deduplicated within pruneWindow sequence numbers behind the
// delivery watermark, which covers any retransmission that can still occur.
func newRecvAudit(capacity, pruneWindow uint32) *recvAudit {
	if pruneWindow < 1024 {
		pruneWindow = 1024
	}

	return &recvAudit{
		events:      make(chan RecvDeadlineEvent, capacity),
		pending:     make(map[uint32]recvAuditPending),
		reported:    make(map[uint32]struct{}),
		pruneWindow: pruneWindow,
	}
}

// register records the deadline of a received packet. It must be called for
// every received data packet, including retransmissions, before the packet
// enters the receive congestion control. Packets that arrive after the
// delivery of a later packet are reported as dropped immediately.
func (a *recvAudit) register(seq circular.Number, timestamp uint32, deadline, arrivedAt time.Time) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.closed {
		return
	}

	s := seq.Val()

	if _, ok := a.reported[s]; ok {
		// Retransmission or duplicate of a packet whose fate is already decided.
		return
	}

	if _, ok := a.pending[s]; ok {
		// Retransmission or duplicate of a packet that is still buffered.
		return
	}

	if a.haveDelivered && seq.Lte(a.delivered) {
		// The receiver already delivered past this sequence number and will
		// discard the packet. Report it as dropped right away.
		a.reported[s] = struct{}{}
		a.emit(RecvDeadlineEvent{
			SequenceNumber: s,
			Timestamp:      timestamp,
			Deadline:       deadline,
			ArrivedAt:      arrivedAt,
			Status:         RecvDeadlineDropped,
		})
		a.prune()

		return
	}

	a.pending[s] = recvAuditPending{
		timestamp: timestamp,
		deadline:  deadline,
		arrivedAt: arrivedAt,
		late:      arrivedAt.After(deadline),
	}
}

// deliver records the outcome of a packet that left the receive congestion
// control for the read queue. ok tells whether the packet actually reached
// the read queue. Deliveries happen in sequence order; everything still
// pending below the delivered sequence number was skipped (too-late packet
// drop) and is reported as dropped.
func (a *recvAudit) deliver(seq circular.Number, ok bool, now time.Time) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.closed {
		return
	}

	if a.haveDelivered && seq.Lte(a.delivered) {
		// Cannot happen with the live congestion control, which delivers
		// strictly in order. Ignore defensively.
		return
	}

	a.delivered = seq
	a.haveDelivered = true

	s := seq.Val()

	if e, okp := a.pending[s]; okp {
		delete(a.pending, s)
		a.reported[s] = struct{}{}

		ev := RecvDeadlineEvent{
			SequenceNumber: s,
			Timestamp:      e.timestamp,
			Deadline:       e.deadline,
			ArrivedAt:      e.arrivedAt,
			Status:         RecvDeadlineOnTime,
		}

		switch {
		case !ok:
			ev.Status = RecvDeadlineDropped
		case e.late:
			ev.Status = RecvDeadlineLate
		}

		if ev.Status != RecvDeadlineDropped {
			ev.DeliveredAt = now
		}

		a.emit(ev)
	}

	// Sweep packets that were received but are now behind the delivered
	// sequence number. They were skipped and will never be delivered.
	for ps, e := range a.pending {
		if !circular.New(ps, packet.MAX_SEQUENCENUMBER).Lte(a.delivered) {
			continue
		}

		delete(a.pending, ps)
		a.reported[ps] = struct{}{}

		a.emit(RecvDeadlineEvent{
			SequenceNumber: ps,
			Timestamp:      e.timestamp,
			Deadline:       e.deadline,
			ArrivedAt:      e.arrivedAt,
			Status:         RecvDeadlineDropped,
		})
	}

	a.prune()
}

// close discards all tracked state and closes the event channel. Pending
// packets are not reported: the connection is gone and no delivery decision
// will be made for them. Must only be called once the connection is down.
func (a *recvAudit) close() {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.closed {
		return
	}

	a.closed = true
	a.pending = nil
	a.reported = nil

	close(a.events)
}

// emit puts an event on the queue without blocking. If the consumer is too
// slow and the queue is full, the event is discarded and counted. The caller
// must hold the lock and the audit must not be closed.
func (a *recvAudit) emit(ev RecvDeadlineEvent) {
	select {
	case a.events <- ev:
	default:
		a.droppedEvents.Add(1)
	}
}

// prune bounds the reported set to the pruneWindow most recent sequence
// numbers behind the delivery watermark. The caller must hold the lock.
func (a *recvAudit) prune() {
	if !a.haveDelivered || len(a.reported) <= int(a.pruneWindow) {
		return
	}

	cutoff := a.delivered.Sub(a.pruneWindow)

	for s := range a.reported {
		if circular.New(s, packet.MAX_SEQUENCENUMBER).Lt(cutoff) {
			delete(a.reported, s)
		}
	}
}
