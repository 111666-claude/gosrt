package srt

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

// startRouterServer starts a listener that routes all incoming connections
// through the given router.
func startRouterServer(t *testing.T, router *Router) string {
	t.Helper()

	config := DefaultConfig()

	ln, err := Listen("srt", "127.0.0.1:0", config)
	require.NoError(t, err)

	go func() {
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}

			go func() {
				route, conn, err := router.Accept(req)
				if err != nil {
					return
				}

				router.Serve(route, conn)
				conn.Close()
			}()
		}
	}()

	t.Cleanup(func() {
		ln.Close()
		router.Close()
	})

	return ln.Addr().String()
}

func dialStream(t *testing.T, addr string, sid *StreamId) Conn {
	t.Helper()

	config := DefaultConfig()
	config.StreamId = sid.String()

	conn, err := Dial("srt", addr, config)
	require.NoError(t, err)

	return conn
}

func requireEventually(t *testing.T, cond func() bool, msg string, args ...interface{}) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf(msg, args...)
}

func readMessage(t *testing.T, conn Conn, n int) []byte {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 2048)
	data := bytes.Buffer{}

	for data.Len() < n {
		m, err := conn.Read(buf)
		if m > 0 {
			data.Write(buf[:m])
		}
		if err != nil {
			t.Fatalf("read failed after %d of %d bytes: %v", data.Len(), n, err)
		}
	}

	return data.Bytes()
}

func TestRouterPublishSubscribe(t *testing.T) {
	router := NewRouter(RouterConfig{})
	addr := startRouterServer(t, router)

	message := "Hello viewers!"

	// Two subscribers on the same path, one on a different path
	sub1 := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/match1"})
	defer sub1.Close()
	sub2 := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/match1"})
	defer sub2.Close()
	subOther := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/match2"})
	defer subOther.Close()

	requireEventually(t, func() bool {
		return router.SubscriberCount("live/match1") == 2 && router.SubscriberCount("live/match2") == 1
	}, "subscribers did not register")

	pub := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/match1"})
	defer pub.Close()

	// The raw streamid must still be visible on the connection
	require.Equal(t, "#!::m=publish,r=live/match1", pub.StreamId())

	n, err := pub.Write([]byte(message))
	require.NoError(t, err)
	require.Equal(t, len(message), n)

	// Both subscribers of the path receive the data
	require.Equal(t, message, string(readMessage(t, sub1, len(message))))
	require.Equal(t, message, string(readMessage(t, sub2, len(message))))

	// The subscriber of the other path must not receive anything
	subOther.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = subOther.Read(make([]byte, 2048))
	require.Error(t, err, "subscriber of a different path must not receive data")

	// Closing the publisher ends the stream for its subscribers
	pub.Close()

	requireEventually(t, func() bool { return !router.HasPublisher("live/match1") }, "publisher slot was not released")

	sub1.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = sub1.Read(make([]byte, 2048))
	require.Error(t, err, "subscriber must see the end of the stream")
}

func TestRouterSecondPublisherRejected(t *testing.T) {
	router := NewRouter(RouterConfig{})
	addr := startRouterServer(t, router)

	pub1 := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a"})

	// A second publisher on the same path is rejected during the handshake
	config := DefaultConfig()
	config.StreamId = (&StreamId{Mode: ModePublish, Resource: "live/a"}).String()
	_, err := Dial("srt", addr, config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejected")

	// A publisher on a different path is fine
	pubOther := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/b"})
	defer pubOther.Close()

	// After the first publisher disconnects, the slot is released
	pub1.Close()
	requireEventually(t, func() bool { return !router.HasPublisher("live/a") }, "publisher slot was not released")

	pub2 := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a"})
	defer pub2.Close()
}

func TestRouterSubscriberQuota(t *testing.T) {
	router := NewRouter(RouterConfig{
		MaxSubscribersPerPath: 1,
	})
	addr := startRouterServer(t, router)

	sub1 := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a"})
	defer sub1.Close()

	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 1 }, "subscriber did not register")

	// The second subscriber exceeds the quota and is rejected
	config := DefaultConfig()
	config.StreamId = (&StreamId{Mode: ModeRequest, Resource: "live/a"}).String()
	_, err := Dial("srt", addr, config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejected")

	// The quota is per path
	subOther := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/b"})
	defer subOther.Close()

	// Disconnecting releases the slot
	sub1.Close()
	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 0 }, "subscriber slot was not released")

	sub2 := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a"})
	defer sub2.Close()
}

func TestRouterConcurrentAdmit(t *testing.T) {
	const maxSubscribers = 10
	const contenders = 100

	router := NewRouter(RouterConfig{
		MaxSubscribersPerPath: maxSubscribers,
	})
	defer router.Close()

	var wg sync.WaitGroup
	routes := make(chan *Route, contenders)

	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := &StreamId{Mode: ModeRequest, Resource: "live/a", User: fmt.Sprintf("user%d", i)}
			route, _, err := router.admit(sid.String(), nil)
			if err == nil {
				routes <- route
			}
		}(i)
	}

	wg.Wait()
	close(routes)

	admitted := []*Route{}
	for route := range routes {
		admitted = append(admitted, route)
	}

	// The quota must not be exceeded under concurrency
	require.Len(t, admitted, maxSubscribers)
	require.Equal(t, maxSubscribers, router.SubscriberCount("live/a"))

	// Only one publisher wins
	var pubRoutes []*Route
	var pubWg sync.WaitGroup
	var pubLock sync.Mutex

	for i := 0; i < 10; i++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			sid := &StreamId{Mode: ModePublish, Resource: "live/pub"}
			route, _, err := router.admit(sid.String(), nil)
			if err == nil {
				pubLock.Lock()
				pubRoutes = append(pubRoutes, route)
				pubLock.Unlock()
			}
		}()
	}

	pubWg.Wait()
	require.Len(t, pubRoutes, 1)

	// Releasing frees the slots again
	for _, route := range admitted {
		route.Release()
	}
	for _, route := range pubRoutes {
		route.Release()
	}

	require.Equal(t, 0, router.SubscriberCount("live/a"))
	require.False(t, router.HasPublisher("live/pub"))
}

func TestRouterSlowSubscriberDropped(t *testing.T) {
	router := NewRouter(RouterConfig{
		SubscriberQueueSize: 4,
	})
	addr := startRouterServer(t, router)

	// A subscriber that never reads. SRT runs over UDP, so this client
	// does not backpressure the server; its ingress queues simply fill
	// up and drop. The important part is that nobody else is affected.
	slow := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a"})
	defer slow.Close()

	// A subscriber that keeps reading
	fast := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a"})
	defer fast.Close()

	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 2 }, "subscribers did not register")

	pub := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a"})
	defer pub.Close()

	// Fill up all queues between server and the slow client
	message := bytes.Repeat([]byte("x"), 1024)
	go func() {
		for i := 0; i < 4096; i++ {
			if _, err := pub.Write(message); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// The fast subscriber keeps receiving
	received := 0
	buf := make([]byte, 2048)
	fast.SetReadDeadline(time.Now().Add(10 * time.Second))

	for received < 32*1024 {
		n, err := fast.Read(buf)
		require.NoError(t, err, "fast subscriber must not be affected by the slow one")
		received += n
	}

	// Disconnecting the slow subscriber releases its slot
	slow.Close()
	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 1 }, "subscriber slot was not released after disconnect")
}

// fakeConn is a Conn implementation for testing the router without
// networking. WritePacket blocks until someone reads from writeCh.
type fakeConn struct {
	readCh    chan packet.Packet
	writeCh   chan packet.Packet
	done      chan struct{}
	closeOnce sync.Once
}

func newFakeConn(queueSize int) *fakeConn {
	return &fakeConn{
		readCh:  make(chan packet.Packet, 4096),
		writeCh: make(chan packet.Packet, queueSize),
		done:    make(chan struct{}),
	}
}

func (c *fakeConn) Read(b []byte) (int, error) { return 0, nil }
func (c *fakeConn) ReadPacket() (packet.Packet, error) {
	select {
	case p := <-c.readCh:
		return p, nil
	case <-c.done:
		return nil, io.EOF
	}
}
func (c *fakeConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *fakeConn) WritePacket(p packet.Packet) error {
	select {
	case c.writeCh <- p:
		return nil
	case <-c.done:
		return io.EOF
	}
}
func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}
func (c *fakeConn) Done() <-chan struct{}              { return c.done }
func (c *fakeConn) LocalAddr() net.Addr                { return nil }
func (c *fakeConn) RemoteAddr() net.Addr               { return nil }
func (c *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *fakeConn) SocketId() uint32                   { return 1 }
func (c *fakeConn) PeerSocketId() uint32               { return 2 }
func (c *fakeConn) StreamId() string                   { return "" }
func (c *fakeConn) Stats(s *Statistics)                {}
func (c *fakeConn) Version() uint32                    { return 5 }

func TestRouterKicksStuckSubscriber(t *testing.T) {
	router := NewRouter(RouterConfig{
		SubscriberQueueSize: 4,
	})
	defer router.Close()

	// A subscriber whose writes never go anywhere
	stuck := newFakeConn(0)
	stuckRoute, _, err := router.admit("#!::m=request,r=live/a", nil)
	require.NoError(t, err)
	stuckServeDone := make(chan error, 1)
	go func() {
		stuckServeDone <- router.Serve(stuckRoute, stuck)
	}()

	// A subscriber that absorbs everything
	healthy := newFakeConn(4096)
	healthyRoute, _, err := router.admit("#!::m=request,r=live/a", nil)
	require.NoError(t, err)
	go router.Serve(healthyRoute, healthy)

	pubRoute, _, err := router.admit("#!::m=publish,r=live/a", nil)
	require.NoError(t, err)
	pub := newFakeConn(0)
	pubServeDone := make(chan error, 1)
	go func() {
		pubServeDone <- router.Serve(pubRoute, pub)
	}()

	// Let the serve loops start before feeding
	time.Sleep(100 * time.Millisecond)

	// Feed the publisher, paced so the healthy subscriber can keep up
	const packets = 100
	go func() {
		for i := 0; i < packets; i++ {
			p := packet.NewPacket(nil)
			p.SetData([]byte("payload"))
			pub.readCh <- p
			time.Sleep(time.Millisecond)
		}
	}()

	// The stuck subscriber is kicked and its slot released
	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 1 }, "stuck subscriber was not kicked")
	require.ErrorIs(t, <-stuckServeDone, ErrSubscriberTooSlow)

	// The healthy subscriber received every packet. The packets have
	// already been decommissioned by the router after WritePacket.
	for i := 0; i < packets; i++ {
		select {
		case <-healthy.writeCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("healthy subscriber received only %d of %d packets", i, packets)
		}
	}

	// The publisher is still going
	require.True(t, router.HasPublisher("live/a"), "publisher must still be active")

	pub.Close()
	require.ErrorIs(t, <-pubServeDone, io.EOF)
}

func TestRouterPublisherIdleTimeout(t *testing.T) {
	router := NewRouter(RouterConfig{
		PublisherIdleTimeout: 500 * time.Millisecond,
	})
	addr := startRouterServer(t, router)

	// A publisher that doesn't send anything
	pub := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a"})
	defer pub.Close()

	require.True(t, router.HasPublisher("live/a"))

	// The idle timeout ends the publication and frees the slot
	requireEventually(t, func() bool { return !router.HasPublisher("live/a") }, "idle publisher was not timed out")

	pub2 := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a"})
	defer pub2.Close()
}

func TestRouterAuthorize(t *testing.T) {
	router := NewRouter(RouterConfig{
		Authorize: func(req AuthRequest) RejectionReason {
			if req.Mode == PUBLISH && req.Password != "s3cret" {
				return REJX_UNAUTHORIZED
			}
			return 0
		},
	})
	addr := startRouterServer(t, router)

	// Publishing without the right credential is rejected
	config := DefaultConfig()
	config.StreamId = (&StreamId{Mode: ModePublish, Resource: "live/a", User: "camera1"}).String()
	_, err := Dial("srt", addr, config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejected")

	// With the credential it works
	pub := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a", User: "camera1", Password: "s3cret"})
	defer pub.Close()

	// Subscribing is not protected
	sub := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a"})
	defer sub.Close()
}

func TestRouterRejectsInvalidStreamId(t *testing.T) {
	router := NewRouter(RouterConfig{})
	addr := startRouterServer(t, router)

	for _, streamid := range []string{
		"foobar",                       // not structured
		"#!::m=publish",                // missing resource
		"#!::r=live/a",                 // missing mode
		"#!::m=bidirectional,r=live/a", // unsupported mode
		"#!::m=publish,r=a,r=b",        // duplicate key
	} {
		config := DefaultConfig()
		config.StreamId = streamid
		_, err := Dial("srt", addr, config)
		require.Error(t, err, streamid)
		require.Contains(t, err.Error(), "rejected", streamid)
	}
}

func TestRouterNoCredentialLogging(t *testing.T) {
	logger := NewLogger([]string{"router", "connection"})

	router := NewRouter(RouterConfig{
		Logger: logger,
	})
	addr := startRouterServer(t, router)

	pub := dialStream(t, addr, &StreamId{Mode: ModePublish, Resource: "live/a", User: "camera1", Password: "s3cret"})
	sub := dialStream(t, addr, &StreamId{Mode: ModeRequest, Resource: "live/a", User: "viewer1", Password: "t0ps3cret"})

	requireEventually(t, func() bool { return router.SubscriberCount("live/a") == 1 }, "subscriber did not register")

	pub.Close()
	sub.Close()

	requireEventually(t, func() bool { return !router.HasPublisher("live/a") }, "publisher slot was not released")

	// Drain the log
	time.Sleep(100 * time.Millisecond)

	for {
		select {
		case entry := <-logger.Listen():
			require.NotContains(t, entry.Message, "s3cret", "log topic %s", entry.Topic)
			require.NotContains(t, entry.Message, "t0ps3cret", "log topic %s", entry.Topic)
		default:
			return
		}
	}
}

func TestRouterClosed(t *testing.T) {
	router := NewRouter(RouterConfig{})
	router.Close()

	sid := &StreamId{Mode: ModePublish, Resource: "live/a"}
	_, _, err := router.admit(sid.String(), nil)
	require.ErrorIs(t, err, ErrRouterClosed)
}
