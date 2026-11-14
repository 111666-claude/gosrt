package live

import (
	"net"
	"testing"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

func mockLiveRecv(onSendACK func(seq circular.Number, light bool), onSendNAK func(list []circular.Number), onDeliver func(p packet.Packet)) *receiver {
	recv := NewReceiver(ReceiveConfig{
		InitialSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER),
		PeriodicACKInterval:   10,
		PeriodicNAKInterval:   20,
		OnSendACK:             onSendACK,
		OnSendNAK:             onSendNAK,
		OnDeliver:             onDeliver,
	})

	return recv.(*receiver)
}

func TestRecvSequence(t *testing.T) {
	nACK := 0
	nNAK := 0
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			nACK++
		},
		func(list []circular.Number) {
			nNAK++
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 10 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, 0, nACK)
	require.Equal(t, 0, nNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())

	recv.Tick(1)

	require.Equal(t, 0, nACK)
	require.Equal(t, 0, nNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())

	recv.Tick(10) // ACK period

	require.Equal(t, 1, nACK)
	require.Equal(t, 0, nNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(9), recv.lastACKSequenceNumber.Val())

	require.Exactly(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, numbers)
}

func TestRecvTSBPD(t *testing.T) {
	numbers := []uint32{}
	recv := mockLiveRecv(
		nil,
		nil,
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 20 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())

	recv.Tick(10) // ACK period

	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(19), recv.lastACKSequenceNumber.Val())

	require.Exactly(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, numbers)
}

func TestRecvNAK(t *testing.T) {
	seqACK := uint32(0)
	seqNAK := []uint32{}
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		func(list []circular.Number) {
			seqNAK = []uint32{}
			for _, sn := range list {
				seqNAK = append(seqNAK, sn.Val())
			}
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{}, seqNAK)
	require.Equal(t, uint32(4), recv.maxSeenSequenceNumber.Val())

	for i := 7; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())

	recv.Tick(10) // ACK period

	require.Equal(t, uint32(10), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
}

func TestRecvPeriodicNAK(t *testing.T) {
	seqACK := uint32(0)
	seqNAK := []uint32{}
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		func(list []circular.Number) {
			seqNAK = []uint32{}
			for _, sn := range list {
				seqNAK = append(seqNAK, sn.Val())
			}
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(50 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{}, seqNAK)
	require.Equal(t, uint32(4), recv.maxSeenSequenceNumber.Val())

	for i := 7; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(50 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())

	recv.Tick(10) // ACK period

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())

	recv.Tick(20) // ACK period, NAK period

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
}

func TestRecvACK(t *testing.T) {
	seqACK := uint32(0)
	seqNAK := []uint32{}
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		func(list []circular.Number) {
			seqNAK = []uint32{}
			for _, sn := range list {
				seqNAK = append(seqNAK, sn.Val())
			}
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(10 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{}, seqNAK)
	require.Equal(t, uint32(4), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(0), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{}, numbers)

	for i := 7; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(30 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{5, 6}, seqNAK)
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(0), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{}, numbers)

	for i := 15; i < 20; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(30 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint32(0), seqACK)
	require.Equal(t, []uint32{10, 14}, seqNAK)
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(0), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(0), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{}, numbers)

	recv.Tick(10)

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{10, 14}, seqNAK)
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(5), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(0), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{}, numbers)

	recv.Tick(20)

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{5, 6, 10, 14}, seqNAK)
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(5), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(5), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{0, 1, 2, 3, 4}, numbers)

	recv.Tick(30)

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{5, 6, 10, 14}, seqNAK)
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(5), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(5), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{0, 1, 2, 3, 4}, numbers)

	for i := 5; i < 7; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(30 + i + 1)

		recv.Push(p)
	}

	recv.Tick(40)

	require.Equal(t, uint32(10), seqACK)
	require.Equal(t, []uint32{10, 14}, seqNAK)
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint32(10), recv.lastACKSequenceNumber.Inc().Val())
	require.Equal(t, uint32(10), recv.lastDeliveredSequenceNumber.Inc().Val())
	require.Exactly(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, numbers)
}

func TestRecvDropTooLate(t *testing.T) {
	recv := mockLiveRecv(
		nil,
		nil,
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 10 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	recv.Tick(10) // ACK period

	stats := recv.Stats()

	require.Equal(t, uint32(9), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(9), recv.lastDeliveredSequenceNumber.Val())
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint64(0), stats.PktDrop)

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(3), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(4)

	recv.Push(p)

	stats = recv.Stats()

	require.Equal(t, uint64(1), stats.PktDrop)
}

func TestRecvDropAlreadyACK(t *testing.T) {
	recv := mockLiveRecv(
		nil,
		nil,
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	for i := 5; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(10 + i + 1)

		recv.Push(p)
	}

	recv.Tick(10) // ACK period

	stats := recv.Stats()

	require.Equal(t, uint32(9), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(4), recv.lastDeliveredSequenceNumber.Val())
	require.Equal(t, uint32(9), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint64(0), stats.PktDrop)

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(6), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(7)

	recv.Push(p)

	stats = recv.Stats()

	require.Equal(t, uint64(1), stats.PktDrop)
}

func TestRecvDropAlreadyRecvNoACK(t *testing.T) {
	recv := mockLiveRecv(
		nil,
		nil,
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	for i := 5; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(10 + i + 1)

		recv.Push(p)
	}

	recv.Tick(10) // ACK period

	for i := range 10 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(10+i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(20 + i + 1)

		recv.Push(p)
	}

	stats := recv.Stats()

	require.Equal(t, uint32(9), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(4), recv.lastDeliveredSequenceNumber.Val())
	require.Equal(t, uint32(19), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, uint64(0), stats.PktDrop)

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(15), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(20 + 6)

	recv.Push(p)

	stats = recv.Stats()

	require.Equal(t, uint64(1), stats.PktDrop)
}

func mockLiveRecvWithInitial(initial uint32, onSendACK func(seq circular.Number, light bool), onSendNAK func(list []circular.Number), onDeliver func(p packet.Packet)) *receiver {
	recv := NewReceiver(ReceiveConfig{
		InitialSequenceNumber: circular.New(initial, packet.MAX_SEQUENCENUMBER),
		PeriodicACKInterval:   10,
		PeriodicNAKInterval:   20,
		OnSendACK:             onSendACK,
		OnSendNAK:             onSendNAK,
		OnDeliver:             onDeliver,
	})

	return recv.(*receiver)
}

func pushPacket(recv *receiver, addr net.Addr, seq uint32, tsbpdTime uint64) {
	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(seq, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = tsbpdTime

	recv.Push(p)
}

// The sender gives up on a gap of 2 packets after sequence number 101.
// The receiver must advance its ACK position beyond the dropped range and
// must not request the dropped packets again.
func TestRecvDropRequest(t *testing.T) {
	seqACK := uint32(0)
	nakWords := 0
	numbers := []uint32{}
	recv := mockLiveRecvWithInitial(100,
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		func(list []circular.Number) {
			nakWords += len(list)
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	// Receive up to sequence number 101
	for i := 100; i <= 101; i++ {
		pushPacket(recv, addr, uint32(i), uint64(i+1))
	}

	recv.Tick(10) // ACK period

	require.Equal(t, uint32(102), seqACK)
	require.Equal(t, uint32(101), recv.lastACKSequenceNumber.Val())

	// 104 arrives, leaving a gap of 2 packets (102, 103)
	pushPacket(recv, addr, 104, 105)

	require.Equal(t, 2, nakWords) // immediate NAK [102, 103]

	recv.Tick(30) // ACK + NAK period

	require.Equal(t, uint32(101), recv.lastACKSequenceNumber.Val())
	require.Equal(t, 4, nakWords) // repeated NAK [102, 103]

	// The sender gives up on 102 and 103
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(103, packet.MAX_SEQUENCENUMBER))

	recv.Tick(50) // ACK + NAK period

	// The ACK position moved beyond the dropped range up to the received 104
	require.Equal(t, uint32(104), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(105), seqACK)
	// The dropped range is not requested again
	require.Equal(t, 4, nakWords)

	recv.Tick(110)

	// 104 gets delivered, 102 and 103 never
	require.Equal(t, []uint32{100, 101, 104}, numbers)
}

// Same as TestRecvDropRequest but with a gap of 1000 packets.
func TestRecvDropRequestLargeGap(t *testing.T) {
	seqACK := uint32(0)
	nakWords := 0
	recv := mockLiveRecvWithInitial(100,
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		func(list []circular.Number) {
			nakWords += len(list)
		},
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	// Receive up to sequence number 101
	for i := 100; i <= 101; i++ {
		pushPacket(recv, addr, uint32(i), uint64(i+1))
	}

	recv.Tick(10) // ACK period

	require.Equal(t, uint32(101), recv.lastACKSequenceNumber.Val())

	// 1102 arrives, leaving a gap of 1000 packets (102 - 1101)
	pushPacket(recv, addr, 1102, 1103)

	require.Equal(t, 2, nakWords) // immediate NAK [102, 1101]

	recv.Tick(30) // ACK + NAK period

	require.Equal(t, uint32(101), recv.lastACKSequenceNumber.Val())
	require.Equal(t, 4, nakWords) // repeated NAK [102, 1101]

	// The sender gives up on 102 - 1101
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(1101, packet.MAX_SEQUENCENUMBER))

	recv.Tick(50) // ACK + NAK period

	// The ACK position moved beyond the dropped range up to the received 1102
	require.Equal(t, uint32(1102), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(1103), seqACK)
	// The dropped range is not requested again
	require.Equal(t, 4, nakWords)
}

// The dropped range spans the sequence number wrap-around point.
func TestRecvDropRequestBoundary(t *testing.T) {
	nakWords := 0
	recv := mockLiveRecvWithInitial(packet.MAX_SEQUENCENUMBER-1,
		nil,
		func(list []circular.Number) {
			nakWords += len(list)
		},
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	// Receive up to the maximum sequence number
	pushPacket(recv, addr, packet.MAX_SEQUENCENUMBER-1, 1)
	pushPacket(recv, addr, packet.MAX_SEQUENCENUMBER, 2)

	recv.Tick(10) // ACK period

	require.Equal(t, packet.MAX_SEQUENCENUMBER, recv.lastACKSequenceNumber.Val())

	// 2 arrives (after wrap-around), leaving a gap of 2 packets (0, 1)
	pushPacket(recv, addr, 2, 105)

	require.Equal(t, 2, nakWords) // immediate NAK [0, 1]

	recv.Tick(30) // ACK + NAK period

	require.Equal(t, 4, nakWords) // repeated NAK [0, 1]

	// The sender gives up on 0 and 1
	recv.Drop(circular.New(0, packet.MAX_SEQUENCENUMBER), circular.New(1, packet.MAX_SEQUENCENUMBER))

	recv.Tick(50) // ACK + NAK period

	// The ACK position moved beyond the dropped range, across the wrap-around
	require.Equal(t, uint32(2), recv.lastACKSequenceNumber.Val())
	require.Equal(t, 4, nakWords)
}

// Repeated, outdated, or invalid drop requests must not change the state
// of the receiver.
func TestRecvDropRequestStaleAndRepeated(t *testing.T) {
	nakWords := 0
	recv := mockLiveRecvWithInitial(100,
		nil,
		func(list []circular.Number) {
			nakWords += len(list)
		},
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := 100; i <= 101; i++ {
		pushPacket(recv, addr, uint32(i), uint64(i+1))
	}

	recv.Tick(10) // ACK period

	pushPacket(recv, addr, 104, 105)

	// The sender gives up on 102 and 103
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(103, packet.MAX_SEQUENCENUMBER))

	recv.Tick(30) // ACK + NAK period

	require.Equal(t, uint32(104), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(104), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, 2, nakWords) // only the immediate NAK, no periodic NAK

	nPackets := recv.statistics.Pkt
	nPktDrop := recv.statistics.PktDrop
	lenPacketList := recv.packetList.Len()

	// Repeating the same drop request is a no-op
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(103, packet.MAX_SEQUENCENUMBER))

	// A drop request completely below the ACK position is a no-op
	recv.Drop(circular.New(50, packet.MAX_SEQUENCENUMBER), circular.New(60, packet.MAX_SEQUENCENUMBER))

	// A drop request ending at the ACK position is a no-op
	recv.Drop(circular.New(100, packet.MAX_SEQUENCENUMBER), circular.New(104, packet.MAX_SEQUENCENUMBER))

	// An invalid range is a no-op
	recv.Drop(circular.New(103, packet.MAX_SEQUENCENUMBER), circular.New(102, packet.MAX_SEQUENCENUMBER))

	require.Equal(t, uint32(104), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(104), recv.maxSeenSequenceNumber.Val())
	require.Equal(t, nPackets, recv.statistics.Pkt)
	require.Equal(t, nPktDrop, recv.statistics.PktDrop)
	require.Equal(t, lenPacketList, recv.packetList.Len())

	recv.Tick(50) // ACK + NAK period

	require.Equal(t, uint32(104), recv.lastACKSequenceNumber.Val())
	require.Equal(t, 2, nakWords)
}

// A drop request must only affect the dropped range. Packets outside of the
// range stay in the buffer and get delivered, other gaps are still reported,
// and late or retransmitted packets are handled as usual.
func TestRecvDropRequestKeepsBuffer(t *testing.T) {
	seqNAK := []uint32{}
	numbers := []uint32{}
	recv := mockLiveRecvWithInitial(100,
		nil,
		func(list []circular.Number) {
			seqNAK = []uint32{}
			for _, sn := range list {
				seqNAK = append(seqNAK, sn.Val())
			}
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := 100; i <= 101; i++ {
		pushPacket(recv, addr, uint32(i), uint64(i+1))
	}

	recv.Tick(10) // ACK period

	pushPacket(recv, addr, 104, 105) // gap of 102, 103
	pushPacket(recv, addr, 105, 106)
	pushPacket(recv, addr, 108, 109) // gap of 106, 107

	// 102 arrives late and fills a part of the first gap
	pushPacket(recv, addr, 102, 103)

	require.Equal(t, 6, recv.packetList.Len()) // 100, 101, 102, 104, 105, 108

	// The sender gives up on 102 and 103
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(103, packet.MAX_SEQUENCENUMBER))

	// Only 102 has been removed from the buffer, everything else stays
	require.Equal(t, 5, recv.packetList.Len()) // 100, 101, 104, 105, 108
	require.Equal(t, uint64(1), recv.statistics.PktDrop)

	recv.Tick(30) // ACK + NAK period

	// The ACK position moved beyond the dropped range and the received 104, 105
	require.Equal(t, uint32(105), recv.lastACKSequenceNumber.Val())
	// The other gap is still reported
	require.Equal(t, []uint32{106, 107}, seqNAK)

	// A retransmitted 102 arrives too late and gets dropped
	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(102, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 103
	p.Header().RetransmittedPacketFlag = true

	recv.Push(p)

	require.Equal(t, uint64(2), recv.statistics.PktDrop)
	require.Equal(t, 5, recv.packetList.Len())

	// 106 arrives late and fills a part of the second gap
	pushPacket(recv, addr, 106, 107)

	require.Equal(t, 6, recv.packetList.Len())

	recv.Tick(50) // ACK + NAK period

	require.Equal(t, uint32(106), recv.lastACKSequenceNumber.Val())
	// Only the still missing 107 is reported
	require.Equal(t, []uint32{107, 107}, seqNAK)

	recv.Tick(120)

	// Everything except the dropped and lost packets gets delivered
	require.Equal(t, []uint32{100, 101, 104, 105, 106, 108}, numbers)
}

// A drop request can cover packets that haven't been seen yet. Those must
// not be reported as lost once later packets arrive.
func TestRecvDropRequestBeyondSeen(t *testing.T) {
	nakWords := 0
	recv := mockLiveRecvWithInitial(100,
		nil,
		func(list []circular.Number) {
			nakWords += len(list)
		},
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := 100; i <= 101; i++ {
		pushPacket(recv, addr, uint32(i), uint64(i+1))
	}

	recv.Tick(10) // ACK period

	pushPacket(recv, addr, 104, 105) // gap of 102, 103

	require.Equal(t, 2, nakWords) // immediate NAK [102, 103]

	// The sender gives up on 102 - 200, reaching beyond what has been seen
	recv.Drop(circular.New(102, packet.MAX_SEQUENCENUMBER), circular.New(200, packet.MAX_SEQUENCENUMBER))

	require.Equal(t, uint32(200), recv.lastACKSequenceNumber.Val())
	require.Equal(t, uint32(200), recv.maxSeenSequenceNumber.Val())

	// 201 arrives in order and must not trigger a NAK for the dropped range
	pushPacket(recv, addr, 201, 202)

	require.Equal(t, 2, nakWords)

	recv.Tick(30) // ACK + NAK period

	require.Equal(t, uint32(201), recv.lastACKSequenceNumber.Val())
	require.Equal(t, 2, nakWords)
}

func TestRecvFlush(t *testing.T) {
	recv := mockLiveRecv(
		nil,
		nil,
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 10 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, 10, recv.packetList.Len())

	recv.Flush()

	require.Equal(t, 0, recv.packetList.Len())
}

func TestRecvPeriodicACKLite(t *testing.T) {
	liteACK := false
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			liteACK = light
		},
		nil,
		nil,
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 100 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(10 + i + 1)

		recv.Push(p)
	}

	require.Equal(t, false, liteACK)

	recv.Tick(1)

	require.Equal(t, true, liteACK)
}

func TestSkipTooLate(t *testing.T) {
	seqACK := uint32(0)
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			seqACK = seq.Val()
		},
		nil,
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 5 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	recv.Tick(10)

	require.Equal(t, uint32(5), seqACK)
	require.Equal(t, []uint32{0, 1, 2, 3, 4}, numbers)

	for i := 5; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(3+i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(13 + i + 1)

		recv.Push(p)
	}

	recv.Tick(20)

	require.Equal(t, uint32(13), seqACK)
	require.Equal(t, []uint32{0, 1, 2, 3, 4, 8, 9}, numbers)
}

func TestIssue67(t *testing.T) {
	ackNumbers := []uint32{}
	nakNumbers := [][2]uint32{}
	numbers := []uint32{}
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {
			ackNumbers = append(ackNumbers, seq.Val())
		},
		func(list []circular.Number) {
			nakNumbers = append(nakNumbers, [2]uint32{list[0].Val(), list[1].Val()})
		},
		func(p packet.Packet) {
			numbers = append(numbers, p.Header().PacketSequenceNumber.Val())
		},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(0, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 1

	recv.Push(p)

	recv.Tick(10)
	recv.Tick(20)
	recv.Tick(30)
	recv.Tick(40)
	recv.Tick(50)
	recv.Tick(60)
	recv.Tick(70)
	recv.Tick(80)
	recv.Tick(90)

	require.Equal(t, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1}, ackNumbers)

	p = packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(12, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 121

	recv.Push(p)

	require.Equal(t, [][2]uint32{
		{1, 11},
	}, nakNumbers)

	p = packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(1, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 11

	recv.Push(p)

	p = packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(11, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 111

	recv.Push(p)

	recv.Tick(100)

	require.Equal(t, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 2}, ackNumbers)

	recv.Tick(110)

	require.Equal(t, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2}, ackNumbers)

	recv.Tick(120)

	require.Equal(t, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 13}, ackNumbers)

	recv.Tick(130)

	require.Equal(t, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 13, 13}, ackNumbers)
}

func TestRecvBelated(t *testing.T) {
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {},
		func(list []circular.Number) {},
		func(p packet.Packet) {},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 3 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint64(3), recv.statistics.Pkt)
	require.Equal(t, uint64(3), recv.statistics.PktBuf)
	require.Equal(t, uint64(0), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktBelated)

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(4), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(4 + 1)

	recv.Push(p)

	require.Equal(t, uint64(4), recv.statistics.Pkt)
	require.Equal(t, uint64(4), recv.statistics.PktBuf)
	require.Equal(t, uint64(1), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktBelated)

	recv.Tick(10) // ACK period

	p = packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(3), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(3 + 1)

	recv.Push(p)

	require.Equal(t, uint64(5), recv.statistics.Pkt)
	require.Equal(t, uint64(0), recv.statistics.PktBuf)
	require.Equal(t, uint64(1), recv.statistics.PktLoss)
	require.Equal(t, uint64(1), recv.statistics.PktBelated)
}

func TestRecvLate(t *testing.T) {
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {},
		func(list []circular.Number) {},
		func(p packet.Packet) {},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := range 3 {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint64(3), recv.statistics.Pkt)
	require.Equal(t, uint64(3), recv.statistics.PktBuf)
	require.Equal(t, uint64(0), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktBelated)
	require.Equal(t, uint64(0), recv.statistics.PktLate)

	p := packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(4), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(4 + 1)

	recv.Push(p)

	require.Equal(t, uint64(4), recv.statistics.Pkt)
	require.Equal(t, uint64(4), recv.statistics.PktBuf)
	require.Equal(t, uint64(1), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktBelated)
	require.Equal(t, uint64(0), recv.statistics.PktLate)

	p = packet.NewPacket(addr)
	p.Header().PacketSequenceNumber = circular.New(uint32(3), packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = uint64(3 + 1)

	recv.Push(p)

	require.Equal(t, uint64(5), recv.statistics.Pkt)
	require.Equal(t, uint64(5), recv.statistics.PktBuf)
	require.Equal(t, uint64(1), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktBelated)
	require.Equal(t, uint64(1), recv.statistics.PktLate)
}

func TestRecvLost(t *testing.T) {
	recv := mockLiveRecv(
		func(seq circular.Number, light bool) {},
		func(list []circular.Number) {},
		func(p packet.Packet) {},
	)

	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	for i := 1; i < 5; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint64(4), recv.statistics.Pkt)
	require.Equal(t, uint64(4), recv.statistics.PktBuf)
	require.Equal(t, uint64(1), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktLost)

	for i := 7; i < 10; i++ {
		p := packet.NewPacket(addr)
		p.Header().PacketSequenceNumber = circular.New(uint32(i), packet.MAX_SEQUENCENUMBER)
		p.Header().PktTsbpdTime = uint64(i + 1)

		recv.Push(p)
	}

	require.Equal(t, uint64(7), recv.statistics.Pkt)
	require.Equal(t, uint64(7), recv.statistics.PktBuf)
	require.Equal(t, uint64(3), recv.statistics.PktLoss)
	require.Equal(t, uint64(0), recv.statistics.PktLost)

	recv.Tick(10) // ACK period

	require.Equal(t, uint64(7), recv.statistics.Pkt)
	require.Equal(t, uint64(0), recv.statistics.PktBuf)
	require.Equal(t, uint64(3), recv.statistics.PktLoss)
	require.Equal(t, uint64(3), recv.statistics.PktLost)
}
