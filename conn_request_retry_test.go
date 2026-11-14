package srt

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestListenResentConclusionAfterAccept simulates a high latency viewer that
// resends the SRT conclusion handshake after the request has already been
// accepted and the connection has been established. The resend must neither
// create a second connection nor crash the server ("close of closed channel").
// The connection that was established first must still be closable.
func TestListenResentConclusionAfterAccept(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:6009", DefaultConfig())
	require.NoError(t, err)
	defer ln.Close()

	var deliveries int64
	var deliveriesLock sync.Mutex
	var acceptOnce sync.Once

	acceptDone := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}

			deliveriesLock.Lock()
			deliveries++
			deliveriesLock.Unlock()
			acceptOnce.Do(func() { close(acceptDone) })

			go func(req ConnRequest) {
				conn, err := req.Accept()
				if err != nil {
					return
				}
				// keep the connection alive for the observation window
				time.Sleep(700 * time.Millisecond)
				conn.Close()
			}(req)
		}
	}()

	conn, err := net.Dial("udp", "127.0.0.1:6009")
	require.NoError(t, err)
	defer conn.Close()

	p, _ := sendHandshakePair(t, conn, 55555)

	// wait until the server accepted the first conclusion
	<-acceptDone

	// the high latency viewer resends the very same conclusion
	var buf bytes.Buffer
	require.NoError(t, p.Marshal(&buf))
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	// observation window: the resend must not produce a second delivery
	time.Sleep(500 * time.Millisecond)

	deliveriesLock.Lock()
	require.Equal(t, int64(1), deliveries, "resent conclusion must not be delivered a second time")
	deliveriesLock.Unlock()

	// the original connection must still be closable without a panic
	require.NotPanics(t, func() { ln.Close() })
	<-serverDone
}

// TestListenResentConclusionWhilePending checks that a conclusion that is
// resent while the request is still waiting to be accepted is deduplicated:
// only a single ConnRequest gets delivered.
func TestListenResentConclusionWhilePending(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:6010", DefaultConfig())
	require.NoError(t, err)
	defer ln.Close()

	gotRequest := make(chan ConnRequest, 4)
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}
			gotRequest <- req
		}
	}()

	conn, err := net.Dial("udp", "127.0.0.1:6010")
	require.NoError(t, err)
	defer conn.Close()

	p, _ := sendHandshakePair(t, conn, 55555)

	// resend while still pending
	var buf bytes.Buffer
	require.NoError(t, p.Marshal(&buf))
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	select {
	case req := <-gotRequest:
		conn2, err := req.Accept()
		require.NoError(t, err)
		conn2.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive the pending request")
	}

	// no second request may be delivered
	select {
	case req := <-gotRequest:
		t.Fatalf("received a duplicate request: %v", req)
	case <-time.After(500 * time.Millisecond):
	}

	ln.Close()
	<-serverDone
}

// TestListenResentConclusionAfterClose checks a conclusion resent after the
// accepted connection has already been closed. It must not crash and must not
// be delivered while the old connection is still shutting down. Once the
// listener has cleaned up the peer, a genuine retry must still be allowed so
// that high latency clients are not permanently locked out.
func TestListenResentConclusionAfterClose(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:6011", DefaultConfig())
	require.NoError(t, err)
	defer ln.Close()

	gotRequest := make(chan ConnRequest, 4)
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}
			gotRequest <- req
		}
	}()

	conn, err := net.Dial("udp", "127.0.0.1:6011")
	require.NoError(t, err)
	defer conn.Close()

	p, _ := sendHandshakePair(t, conn, 55555)

	req := <-gotRequest
	srvConn, err := req.Accept()
	require.NoError(t, err)
	require.NotPanics(t, func() { srvConn.Close() })

	// wait until the shutdown removed the connection from the listener maps
	require.Eventually(t, func() bool {
		lnImpl := ln.(*listener)
		lnImpl.lock.RLock()
		_, tracked := lnImpl.connsByPeer[newPeerKey(req.RemoteAddr(), req.PeerSocketId())]
		lnImpl.lock.RUnlock()
		return !tracked
	}, 2*time.Second, 20*time.Millisecond, "peer must be untracked after the connection closed")

	// Resend the conclusion after the connection was closed: it must not
	// crash the listener.
	var buf bytes.Buffer
	require.NoError(t, p.Marshal(&buf))
	require.NotPanics(t, func() {
		_, err := conn.Write(buf.Bytes())
		require.NoError(t, err)
		time.Sleep(300 * time.Millisecond)
	})

	// The listener must accept the genuine retry as a new request instead of
	// dropping retries unconditionally.
	select {
	case retryReq := <-gotRequest:
		retryConn, err := retryReq.Accept()
		require.NoError(t, err)
		retryConn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("a genuine retry after close must still be accepted")
	}

	ln.Close()
	<-serverDone
}

// TestListenResentConclusionConcurrent drives several peers that retransmit
// their conclusion handshakes concurrently while their connections are being
// accepted. Each peer must end up with exactly one delivered request and one
// connection, and no panic may occur. This guards against cross-talk between
// peer identities, socket id reservations and their release under concurrency.
func TestListenResentConclusionConcurrent(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:6013", DefaultConfig())
	require.NoError(t, err)
	defer ln.Close()

	const peers = 6

	var deliveries int64
	var accepted int64
	serverDone := make(chan struct{})
	allAccepted := make(chan struct{})

	go func() {
		defer close(serverDone)
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}
			atomic.AddInt64(&deliveries, 1)
			go func(req ConnRequest) {
				conn, err := req.Accept()
				if err != nil {
					return
				}
				// Hold the connection open for the whole retransmission window.
				time.Sleep(400 * time.Millisecond)
				if atomic.AddInt64(&accepted, 1) == int64(peers) {
					close(allAccepted)
				}
				conn.Close()
			}(req)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < peers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("udp", "127.0.0.1:6013")
			require.NoError(t, err)
			defer conn.Close()

			// Distinct caller socket ids, mirroring independent devices.
			p, _ := sendHandshakePair(t, conn, 55555+uint32(i))

			// Retransmit the conclusion while the connection is establishing
			// and then alive. None of these may create a second delivery.
			for j := 0; j < 10; j++ {
				var buf bytes.Buffer
				require.NoError(t, p.Marshal(&buf))
				if _, err := conn.Write(buf.Bytes()); err != nil {
					return
				}
				time.Sleep(15 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("deliveries=%d accepted=%d", atomic.LoadInt64(&deliveries), atomic.LoadInt64(&accepted))
	select {
	case <-allAccepted:
	case <-time.After(2 * time.Second):
		t.Fatalf("not all peers accepted: deliveries=%d accepted=%d",
			atomic.LoadInt64(&deliveries), atomic.LoadInt64(&accepted))
	}

	// Give any wrongly delivered duplicate time to surface.
	time.Sleep(300 * time.Millisecond)

	require.Equal(t, int64(peers), atomic.LoadInt64(&deliveries),
		"each peer must be delivered exactly once during an active session")

	require.NotPanics(t, func() { ln.Close() })
	<-serverDone
}

// TestListenDistinctPeersSameSocketId verifies that the peer identity used for
// de-duplication is the combination of remote address and caller socket ID. Two
// independent callers on different local UDP sockets are free to choose the same
// caller socket ID; both must get a connection request instead of the second one
// being swallowed as a retransmission of the first.
func TestListenDistinctPeersSameSocketId(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:6014", DefaultConfig())
	require.NoError(t, err)
	defer ln.Close()

	gotRequest := make(chan ConnRequest, 8)
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}
			gotRequest <- req
		}
	}()

	const sameSocketId = uint32(77777)
	const callers = 2

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("udp", "127.0.0.1:6014")
			require.NoError(t, err)
			defer conn.Close()

			_, _ = sendHandshakePair(t, conn, sameSocketId)
		}()
	}
	wg.Wait()

	received := make(map[string]struct{})
	deadline := time.After(2 * time.Second)
	for len(received) < callers {
		select {
		case req := <-gotRequest:
			received[req.RemoteAddr().String()] = struct{}{}
			conn, err := req.Accept()
			require.NoError(t, err)
			conn.Close()
		case <-deadline:
			t.Fatalf("expected %d distinct peers, got %d", callers, len(received))
		}
	}

	// No additional request from either caller.
	select {
	case req := <-gotRequest:
		t.Fatalf("unexpected extra request from %s", req.RemoteAddr())
	case <-time.After(300 * time.Millisecond):
	}

	ln.Close()
	<-serverDone
}
