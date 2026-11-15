package srt

import (
	"bytes"
	"context"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datarhei/gosrt/packet"

	"github.com/stretchr/testify/require"
)

// startCandidateListener starts a listener for candidate tests and returns its
// address. Accepted connections are delivered on the returned channel. With
// mode REJECT every incoming request is rejected instead.
func startCandidateListener(t *testing.T, mode ConnType) (string, <-chan Conn) {
	t.Helper()

	ln, err := Listen("srt", "127.0.0.1:0", DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(ln.Close)

	accepted := make(chan Conn, 4)

	go func() {
		for {
			conn, _, err := ln.Accept(func(req ConnRequest) ConnType {
				return mode
			})
			if err != nil {
				return
			}

			if conn != nil {
				select {
				case accepted <- conn:
				default:
					conn.Close()
				}
			}
		}
	}()

	return ln.Addr().String(), accepted
}

// startSlowCandidateListener starts a listener that only starts answering
// handshakes after delay. Until then the caller receives no response at all,
// which is what makes the entry point slow.
func startSlowCandidateListener(t *testing.T, delay time.Duration) (string, <-chan Conn) {
	t.Helper()

	ln, err := Listen("srt", "127.0.0.1:0", DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(ln.Close)

	accepted := make(chan Conn, 4)

	go func() {
		time.Sleep(delay)

		for {
			conn, _, err := ln.Accept(func(req ConnRequest) ConnType {
				return SUBSCRIBE
			})
			if err != nil {
				return
			}

			if conn != nil {
				select {
				case accepted <- conn:
				default:
					conn.Close()
				}
			}
		}
	}()

	return ln.Addr().String(), accepted
}

// startBlackhole binds a UDP socket that reads and discards every packet but
// never answers. Sending to it looks like a working network path without any
// SRT peer behind it, i.e. a blackhole.
func startBlackhole(t *testing.T) (string, <-chan struct{}) {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { pc.Close() })

	seen := make(chan struct{}, 16)

	go func() {
		buffer := make([]byte, MAX_MSS_SIZE)
		for {
			if _, _, err := pc.ReadFrom(buffer); err != nil {
				return
			}

			select {
			case seen <- struct{}{}:
			default:
			}
		}
	}()

	return pc.LocalAddr().String(), seen
}

// startInductionOnlyResponder starts a fake SRT peer that answers the
// INDUCTION handshake but never answers the CONCLUSION handshake. The caller
// therefore exchanges UDP packets with it, but the SRT handshake never
// completes.
func startInductionOnlyResponder(t *testing.T) (string, <-chan struct{}) {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { pc.Close() })

	inductionAnswered := make(chan struct{}, 1)

	go func() {
		buffer := make([]byte, MAX_MSS_SIZE)

		for {
			n, addr, err := pc.ReadFrom(buffer)
			if err != nil {
				return
			}

			p, err := packet.NewPacketFromData(addr, buffer[:n])
			if err != nil {
				continue
			}

			cif := &packet.CIFHandshake{}
			if err := p.UnmarshalCIF(cif); err != nil {
				continue
			}

			// Answer only the induction, which makes this candidate look
			// alive without ever completing the handshake.
			if cif.HandshakeType != packet.HSTYPE_INDUCTION {
				continue
			}

			p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
			p.Header().SubType = 0
			p.Header().TypeSpecific = 0
			p.Header().Timestamp = 0
			p.Header().DestinationSocketId = cif.SRTSocketId

			response := &packet.CIFHandshake{
				IsRequest:                   false,
				Version:                     5,
				EncryptionField:             0,
				ExtensionField:              0x4A17,
				InitialPacketSequenceNumber: cif.InitialPacketSequenceNumber,
				MaxTransmissionUnitSize:     cif.MaxTransmissionUnitSize,
				MaxFlowWindowSize:           cif.MaxFlowWindowSize,
				HandshakeType:               packet.HSTYPE_INDUCTION,
				SRTSocketId:                 9876,
				SynCookie:                   0xdeadbeef,
			}

			var data bytes.Buffer
			if err := p.MarshalCIF(response); err != nil {
				continue
			}

			p.Marshal(&data)
			pc.WriteTo(data.Bytes(), addr)

			select {
			case inductionAnswered <- struct{}{}:
			default:
			}
		}
	}()

	return pc.LocalAddr().String(), inductionAnswered
}

// requireHandshake verifies that conn completed the SRT handshake by
// exchanging an actual payload with the peer.
func requireHandshake(t *testing.T, conn Conn, peer Conn) {
	t.Helper()

	require.NotNil(t, conn)
	require.NotZero(t, conn.SocketId(), "the returned connection has no socket id")
	require.NotZero(t, conn.PeerSocketId(), "the returned connection has no peer socket id")

	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))

	payload := []byte("candidate handshake check")

	n, err := conn.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	buffer := make([]byte, 256)
	n, err = peer.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, payload, buffer[:n])
}

func candidateConfig() Config {
	config := DefaultConfig()
	config.ConnectionTimeout = 10 * time.Second
	config.StreamId = "candidates:test"
	return config
}

func TestDialCandidatesRequiresCompletedHandshake(t *testing.T) {
	partialAddr, inductionAnswered := startInductionOnlyResponder(t)
	backupAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	config := candidateConfig()
	config.ConnectionTimeout = 600 * time.Millisecond

	start := time.Now()

	conn, err := DialCandidates("srt", []string{partialAddr, backupAddr}, config, 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	elapsed := time.Since(start)

	// The partial candidate exchanged packets, so it is not a blackhole and
	// the dial has to wait for its timeout. Still, it must never be returned
	// as the result.
	require.Equal(t, backupAddr, conn.RemoteAddr().String())
	require.Less(t, elapsed, 2*time.Second, "the partial candidate delayed the dial by %s", elapsed)

	select {
	case <-inductionAnswered:
	default:
		t.Fatal("the partial candidate never received the induction")
	}

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the backup listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)

	// Used on its own, the partial candidate must fail as a timeout.
	config.ConnectionTimeout = 300 * time.Millisecond

	conn, err = DialCandidates("srt", []string{partialAddr}, config, time.Hour)
	require.Error(t, err)
	require.Nil(t, conn)
	require.ErrorContains(t, err, "connection timeout")
}

func TestDialCandidatesBlackhole(t *testing.T) {
	blackholeAddr, _ := startBlackhole(t)
	backupAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	start := time.Now()

	conn, err := DialCandidates("srt", []string{blackholeAddr, backupAddr}, candidateConfig(), 200*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	elapsed := time.Since(start)

	require.Equal(t, backupAddr, conn.RemoteAddr().String())
	require.Less(t, elapsed, 3*time.Second, "the blackhole candidate delayed the dial by %s", elapsed)

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the backup listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)
}

func TestDialCandidatesSlowPrimary(t *testing.T) {
	primaryAddr, _ := startSlowCandidateListener(t, time.Second)
	backupAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	start := time.Now()

	conn, err := DialCandidates("srt", []string{primaryAddr, backupAddr}, candidateConfig(), 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	elapsed := time.Since(start)

	require.Equal(t, backupAddr, conn.RemoteAddr().String())
	require.Less(t, elapsed, 900*time.Millisecond, "the slow candidate delayed the dial by %s", elapsed)

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the backup listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)
}

func TestDialCandidatesThrottlesAcrossSeveralCandidates(t *testing.T) {
	firstAddr, _ := startBlackhole(t)
	secondAddr, _ := startBlackhole(t)
	thirdAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	config := candidateConfig()

	// Every candidate is blackholed except the last one. The attempts have to
	// be spread over the interval, which only works if the timer is re-armed
	// after each attempt.
	start := time.Now()

	conn, err := DialCandidates("srt", []string{firstAddr, secondAddr, thirdAddr}, config, 150*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	elapsed := time.Since(start)

	require.Equal(t, thirdAddr, conn.RemoteAddr().String())
	require.GreaterOrEqual(t, elapsed, 300*time.Millisecond, "the candidates were not spaced by the interval")
	require.Less(t, elapsed, 3*time.Second)

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the last listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)
}

func TestDialCandidatesRejectionFailsOverImmediately(t *testing.T) {
	rejectAddr, _ := startCandidateListener(t, REJECT)
	backupAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	start := time.Now()

	// The interval is much larger than the test timeout. The rejection of the
	// first candidate has to move on to the next candidate right away.
	conn, err := DialCandidates("srt", []string{rejectAddr, backupAddr}, candidateConfig(), 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	elapsed := time.Since(start)

	require.Equal(t, backupAddr, conn.RemoteAddr().String())
	require.Less(t, elapsed, 3*time.Second, "the rejection did not fail over immediately (%s)", elapsed)

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the backup listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)
}

func TestDialCandidatesConcurrentSuccess(t *testing.T) {
	firstAddr, firstAccepted := startCandidateListener(t, SUBSCRIBE)
	secondAddr, secondAccepted := startCandidateListener(t, SUBSCRIBE)

	// An interval of zero starts all candidates at the same time.
	conn, err := DialCandidates("srt", []string{firstAddr, secondAddr}, candidateConfig(), 0)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	winnerAddr := conn.RemoteAddr().String()
	require.Contains(t, []string{firstAddr, secondAddr}, winnerAddr)

	// Each listener may have accepted a connection. The one that matches the
	// winning candidate is the live peer, every other one must have been
	// closed by DialCandidates before it returned.
	var winnerPeer Conn
	for i := 0; i < 2; i++ {
		var peer Conn

		select {
		case peer = <-firstAccepted:
		case peer = <-secondAccepted:
		case <-time.After(time.Second):
			i = 2
			continue
		}

		if peer.LocalAddr().String() == winnerAddr {
			winnerPeer = peer
			continue
		}

		// A losing connection must not carry data anymore.
		require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
		_, err := peer.Read(make([]byte, 16))
		require.Error(t, err, "the losing connection was not closed")
		peer.Close()
	}

	require.NotNil(t, winnerPeer, "no accepted connection matches the winning candidate")
	defer winnerPeer.Close()

	requireHandshake(t, conn, winnerPeer)
}

func TestDialCandidatesDuplicatesAreAttemptedOnce(t *testing.T) {
	addr, accepted := startCandidateListener(t, SUBSCRIBE)

	var accepts atomic.Int32

	conn, err := DialCandidates("srt", []string{addr, addr, " " + addr + " "}, candidateConfig(), 50*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	var peer Conn
	select {
	case peer = <-accepted:
		accepts.Add(1)
	case <-time.After(3 * time.Second):
		t.Fatal("the listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)

	// A duplicate must not produce a second connection attempt.
	select {
	case extra := <-accepted:
		extra.Close()
		accepts.Add(1)
	case <-time.After(300 * time.Millisecond):
	}

	require.Equal(t, int32(1), accepts.Load())
}

func TestDialCandidatesAllFail(t *testing.T) {
	firstAddr, _ := startCandidateListener(t, REJECT)
	secondAddr, _ := startCandidateListener(t, REJECT)

	conn, err := DialCandidates("srt", []string{firstAddr, secondAddr}, candidateConfig(), 100*time.Millisecond)
	require.Error(t, err)
	require.Nil(t, conn)

	require.ErrorIs(t, err, ErrAllCandidatesFailed)

	var candidatesErr *CandidatesError
	require.ErrorAs(t, err, &candidatesErr)
	require.Len(t, candidatesErr.Attempts, 2)

	// The failure identifies every candidate by its address and reason.
	for i, address := range []string{firstAddr, secondAddr} {
		require.Equal(t, address, candidatesErr.Attempts[i].Address)
		require.Error(t, candidatesErr.Attempts[i].Err)
		require.Contains(t, err.Error(), address)
	}
}

func TestDialCandidatesAllTimeout(t *testing.T) {
	firstAddr, firstSeen := startBlackhole(t)
	secondAddr, secondSeen := startBlackhole(t)

	config := candidateConfig()
	config.ConnectionTimeout = 300 * time.Millisecond

	conn, err := DialCandidates("srt", []string{firstAddr, secondAddr}, config, 100*time.Millisecond)
	require.Error(t, err)
	require.Nil(t, conn)
	require.ErrorIs(t, err, ErrAllCandidatesFailed)

	var candidatesErr *CandidatesError
	require.ErrorAs(t, err, &candidatesErr)
	require.Len(t, candidatesErr.Attempts, 2)

	require.Contains(t, candidatesErr.Attempts[0].Err.Error(), "connection timeout")
	require.Contains(t, candidatesErr.Attempts[1].Err.Error(), "connection timeout")

	// Both candidates were really attempted.
	require.Eventually(t, func() bool {
		return len(firstSeen) > 0 && len(secondSeen) > 0
	}, time.Second, 10*time.Millisecond)
}

func TestDialCandidatesInvalidAddress(t *testing.T) {
	validAddr, _ := startCandidateListener(t, SUBSCRIBE)

	testCases := []struct {
		name      string
		addresses []string
	}{
		{name: "no port", addresses: []string{validAddr, "127.0.0.1"}},
		{name: "no host", addresses: []string{":6000"}},
		{name: "empty", addresses: []string{""}},
		{name: "not a port", addresses: []string{"127.0.0.1:http"}},
		{name: "port zero", addresses: []string{"127.0.0.1:0"}},
		{name: "port too big", addresses: []string{"127.0.0.1:70000"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			conn, err := DialCandidates("srt", testCase.addresses, candidateConfig(), 10*time.Millisecond)
			require.Error(t, err)
			require.Nil(t, conn)
			require.ErrorIs(t, err, ErrInvalidCandidate)
			require.Contains(t, err.Error(), "candidate")
		})
	}
}

func TestDialCandidatesNoAddresses(t *testing.T) {
	conn, err := DialCandidates("srt", nil, candidateConfig(), 10*time.Millisecond)
	require.ErrorIs(t, err, ErrNoCandidates)
	require.Nil(t, conn)
}

func TestDialCandidatesInvalidArguments(t *testing.T) {
	_, err := DialCandidates("udp", []string{"127.0.0.1:6000"}, candidateConfig(), time.Millisecond)
	require.Error(t, err)

	_, err = DialCandidates("srt", []string{"127.0.0.1:6000"}, candidateConfig(), -time.Millisecond)
	require.Error(t, err)

	_, err = DialCandidatesWithContext(nil, "srt", []string{"127.0.0.1:6000"}, candidateConfig(), time.Millisecond)
	require.Error(t, err)

	config := candidateConfig()
	config.TransmissionType = "file"
	_, err = DialCandidates("srt", []string{"127.0.0.1:6000"}, config, time.Millisecond)
	require.Error(t, err)
}

func TestDialCandidatesContextCancelReleasesAttempts(t *testing.T) {
	firstAddr, firstSeen := startBlackhole(t)
	secondAddr, secondSeen := startBlackhole(t)

	// Take the baseline after the blackhole goroutines have been started.
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialDone := make(chan error, 1)

	go func() {
		conn, err := DialCandidatesWithContext(ctx, "srt", []string{firstAddr, secondAddr}, candidateConfig(), 0)
		if conn != nil {
			conn.Close()
		}
		dialDone <- err
	}()

	require.Eventually(t, func() bool {
		return len(firstSeen) > 0 && len(secondSeen) > 0
	}, 3*time.Second, 10*time.Millisecond, "not all candidates were attempted")

	cancel()

	select {
	case err := <-dialDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("a canceled dial did not return")
	}

	// The canceled attempts must release their sockets and goroutines.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+1
	}, 3*time.Second, 20*time.Millisecond, "goroutines leaked: before %d, after %d", before, runtime.NumGoroutine())
}

func TestDialCandidatesCancelBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	blackholeAddr, seen := startBlackhole(t)

	conn, err := DialCandidatesWithContext(ctx, "srt", []string{blackholeAddr}, candidateConfig(), 10*time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, conn)

	select {
	case <-seen:
		t.Fatal("a canceled dial attempted to connect")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDialCandidatesSingleAddress(t *testing.T) {
	rejectAddr, _ := startCandidateListener(t, REJECT)

	conn, err := DialCandidates("srt", []string{rejectAddr}, candidateConfig(), time.Hour)
	require.Error(t, err)
	require.Nil(t, conn)
	require.ErrorIs(t, err, ErrAllCandidatesFailed)

	// As with multiple addresses, the single failure names the candidate.
	require.Contains(t, err.Error(), rejectAddr)
	require.Contains(t, err.Error(), "connection rejected")

	backupAddr, accepted := startCandidateListener(t, SUBSCRIBE)

	conn, err = DialCandidates("srt", []string{backupAddr}, candidateConfig(), time.Hour)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	var peer Conn
	select {
	case peer = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("the listener did not accept the connection")
	}
	defer peer.Close()

	requireHandshake(t, conn, peer)
}

func TestNormalizeCandidateAddresses(t *testing.T) {
	addresses, err := normalizeCandidateAddresses([]string{
		"127.0.0.1:6000",
		" 127.0.0.1:6000 ",
		"[::1]:6001",
		"localhost:6002",
		"127.0.0.1:6000",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:6000", "[::1]:6001", "localhost:6002"}, addresses)

	_, err = normalizeCandidateAddresses([]string{"127.0.0.1:6000", "invalid"})
	require.ErrorIs(t, err, ErrInvalidCandidate)
	require.Contains(t, err.Error(), "candidate 2")
	require.Contains(t, err.Error(), "invalid")
}

func TestCandidatesErrorFormatting(t *testing.T) {
	err := &CandidatesError{
		Attempts: []CandidateError{
			{Address: "10.0.0.1:3000", Err: errors.New("connection rejected")},
			{Address: "10.0.0.2:3000", Err: errors.New("connection timeout")},
		},
	}

	require.ErrorIs(t, err, ErrAllCandidatesFailed)
	require.Contains(t, err.Error(), "10.0.0.1:3000: connection rejected")
	require.Contains(t, err.Error(), "10.0.0.2:3000: connection timeout")

	var candidateErr *CandidateError
	require.ErrorAs(t, err, &candidateErr)

	var target *CandidateError
	require.False(t, errors.As(err, &target) && target.Address == "")
}
