package srt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/datarhei/gosrt/packet"
)

// Errors returned by the Router. All of them are decided and reported
// during the handshake, before a connection is accepted.
var (
	// ErrRouterClosed is returned when the router has been closed.
	ErrRouterClosed = errors.New("srt: router closed")
	// ErrStreamIdInvalid is returned when the streamid is not a valid
	// structured streamid or has no resource.
	ErrStreamIdInvalid = errors.New("srt: invalid streamid")
	// ErrStreamIdUnauthorized is returned when the Authorize hook denied
	// the request.
	ErrStreamIdUnauthorized = errors.New("srt: streamid unauthorized")
	// ErrUnsupportedMode is returned for streamids without a mode or with
	// a mode the router doesn't serve (anything but publish or request).
	ErrUnsupportedMode = errors.New("srt: unsupported mode")
	// ErrPublisherConflict is returned when the path already has a publisher.
	ErrPublisherConflict = errors.New("srt: path already has a publisher")
	// ErrTooManySubscribers is returned when the subscriber quota of the
	// path is exhausted.
	ErrTooManySubscribers = errors.New("srt: too many subscribers for path")
	// ErrSubscriberTooSlow is returned from Serve when a subscriber could
	// not keep up with the publisher and has been dropped.
	ErrSubscriberTooSlow = errors.New("srt: subscriber too slow")
)

// AuthRequest is passed to the RouterConfig.Authorize hook.
type AuthRequest struct {
	Path       string    // the resource path from the streamid
	Mode       ConnType  // PUBLISH or SUBSCRIBE
	User       string    // the "u" key of the streamid, may be empty
	Password   string    // the "pass" key of the streamid, may be empty
	RemoteAddr net.Addr  // address of the peer
	StreamId   *StreamId // the full parsed streamid
}

// RouterConfig configures a Router.
type RouterConfig struct {
	// MaxSubscribersPerPath limits the number of concurrent subscribers
	// per path. 0 means no limit.
	MaxSubscribersPerPath int

	// SubscriberQueueSize is the number of packets buffered per subscriber.
	// If the buffer is full, the oldest packet is dropped in favor of the
	// newest one; a subscriber that persistently falls behind is dropped
	// entirely, without affecting the publisher or other subscribers.
	// Defaults to 256.
	SubscriberQueueSize int

	// PublisherIdleTimeout ends a publication if the publisher doesn't
	// send any data within this duration. 0 disables the timeout.
	PublisherIdleTimeout time.Duration

	// Authorize, if set, is called during the handshake after the streamid
	// has been parsed. Return a non-zero rejection reason to deny the
	// request, or 0 to allow it.
	Authorize func(req AuthRequest) RejectionReason

	// Logger is an optional logger. Only the path, mode, and peer address
	// are logged, never credentials.
	Logger Logger
}

// Router routes SRT connections by the resource path of a structured
// streamid. Connections publishing to the same path are fanned out to all
// connections subscribed to that path. Each path allows exactly one
// publisher and a configurable number of subscribers. All decisions are
// made during the handshake; conflicts and quota violations reject the
// connection request before it is accepted.
//
// The Router is optional and builds on top of the regular Listener API:
// raw streamids (ConnRequest.StreamId) keep working untouched.
type Router struct {
	config RouterConfig
	logger Logger

	lock    sync.Mutex
	streams map[string]*routerStream
	closed  bool
}

// routerStream holds the state of one path.
type routerStream struct {
	path        string
	publisher   bool
	subscribers map[uint32]*routerSubscriber
	nextSubId   uint32

	ctx    context.Context
	cancel context.CancelFunc
}

// routerSubscriber is one subscribed connection of a path.
type routerSubscriber struct {
	queue  chan packet.Packet
	done   chan struct{} // closed when the subscriber has been kicked
	conn   Conn          // set once Serve started, used to unblock a kicked subscriber
	kicked bool
	drops  int // packets dropped since the last clean send
}

// Route describes an admitted connection. It is returned by Router.Admit
// or Router.Accept and must be passed to Router.Serve, or released with
// Route.Release if the connection will not be served.
type Route struct {
	// Path is the resource path of the streamid.
	Path string
	// Mode is PUBLISH or SUBSCRIBE.
	Mode ConnType
	// User is the optional user name from the streamid.
	User string

	router *Router
	stream *routerStream
	sub    *routerSubscriber // subscribers only
	subId  uint32            // subscribers only

	releaseOnce sync.Once
}

// NewRouter returns a new Router.
func NewRouter(config RouterConfig) *Router {
	if config.SubscriberQueueSize <= 0 {
		config.SubscriberQueueSize = 256
	}

	logger := config.Logger
	if logger == nil {
		logger = NewLogger(nil)
	}

	return &Router{
		config:  config,
		logger:  logger,
		streams: make(map[string]*routerStream),
	}
}

// Admit inspects the streamid of a connection request and, if the request
// is allowed, reserves a slot for it. If the request is denied, it is
// rejected with an appropriate reason (the peer sees the rejection as part
// of the handshake) and an error is returned.
//
// A successfully admitted Route must later be handed to Serve, or released
// with Route.Release.
func (r *Router) Admit(req ConnRequest) (*Route, error) {
	route, reason, err := r.admit(req.StreamId(), req.RemoteAddr())
	if err != nil {
		req.Reject(reason)
		return nil, err
	}

	return route, nil
}

// Accept is a convenience wrapper around Admit and ConnRequest.Accept. If
// accepting the connection fails, the reserved slot is released again.
func (r *Router) Accept(req ConnRequest) (*Route, Conn, error) {
	route, err := r.Admit(req)
	if err != nil {
		return nil, nil, err
	}

	conn, err := req.Accept()
	if err != nil {
		route.Release()
		return nil, nil, err
	}

	return route, conn, nil
}

// admit parses the streamid and reserves a slot. It returns the route, the
// rejection reason to use if it fails, and an error.
func (r *Router) admit(raw string, addr net.Addr) (*Route, RejectionReason, error) {
	sid, err := ParseStreamId(raw)
	if err != nil {
		return nil, REJX_BAD_REQUEST, fmt.Errorf("%w: %w", ErrStreamIdInvalid, err)
	}

	if len(sid.Resource) == 0 {
		return nil, REJX_BAD_REQUEST, fmt.Errorf("%w: missing resource", ErrStreamIdInvalid)
	}

	var mode ConnType

	switch sid.Mode {
	case ModePublish:
		mode = PUBLISH
	case ModeRequest:
		mode = SUBSCRIBE
	default:
		return nil, REJX_BAD_MODE, fmt.Errorf("%w: %q", ErrUnsupportedMode, sid.Mode)
	}

	if r.config.Authorize != nil {
		if reason := r.config.Authorize(AuthRequest{
			Path:       sid.Resource,
			Mode:       mode,
			User:       sid.User,
			Password:   sid.Password,
			RemoteAddr: addr,
			StreamId:   sid,
		}); reason != 0 {
			r.log("router:reject", func() string {
				return fmt.Sprintf("authorization denied for path %s from %s", sid.Resource, addrString(addr))
			})
			return nil, reason, fmt.Errorf("%w: path %s", ErrStreamIdUnauthorized, sid.Resource)
		}
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	if r.closed {
		return nil, REJ_CLOSE, ErrRouterClosed
	}

	stream, ok := r.streams[sid.Resource]
	if !ok {
		stream = &routerStream{
			path:        sid.Resource,
			subscribers: make(map[uint32]*routerSubscriber),
		}
		stream.ctx, stream.cancel = context.WithCancel(context.Background())
		r.streams[sid.Resource] = stream
	}

	route := &Route{
		Path:   sid.Resource,
		Mode:   mode,
		User:   sid.User,
		router: r,
		stream: stream,
	}

	if mode == PUBLISH {
		if stream.publisher {
			return nil, REJX_CONFLICT, fmt.Errorf("%w: %s", ErrPublisherConflict, sid.Resource)
		}

		stream.publisher = true

		r.log("router:publish", func() string {
			return fmt.Sprintf("new publisher on path %s from %s", sid.Resource, addrString(addr))
		})
	} else {
		if r.config.MaxSubscribersPerPath > 0 && len(stream.subscribers) >= r.config.MaxSubscribersPerPath {
			return nil, REJX_OVERLOAD, fmt.Errorf("%w: %s (max %d)", ErrTooManySubscribers, sid.Resource, r.config.MaxSubscribersPerPath)
		}

		sub := &routerSubscriber{
			queue: make(chan packet.Packet, r.config.SubscriberQueueSize),
			done:  make(chan struct{}),
		}

		stream.nextSubId++
		route.sub = sub
		route.subId = stream.nextSubId
		stream.subscribers[route.subId] = sub

		r.log("router:subscribe", func() string {
			return fmt.Sprintf("new subscriber on path %s from %s", sid.Resource, addrString(addr))
		})
	}

	return route, 0, nil
}

// Serve runs the data pump for an admitted route. It blocks until the
// connection ends, the publisher of the path disconnects, or (for
// subscribers) the subscriber is dropped for being too slow. The reserved
// slot is released when Serve returns. Serve must be called at most once
// per route.
func (r *Router) Serve(route *Route, conn Conn) error {
	if route.Mode == PUBLISH {
		return r.servePublisher(route, conn)
	}

	return r.serveSubscriber(route, conn)
}

func (r *Router) servePublisher(route *Route, conn Conn) error {
	defer route.Release()

	stream := route.stream
	timeout := r.config.PublisherIdleTimeout

	for {
		if timeout > 0 {
			conn.SetReadDeadline(time.Now().Add(timeout))
		}

		p, err := conn.ReadPacket()
		if err != nil {
			r.log("router:publish", func() string {
				return fmt.Sprintf("publisher on path %s stopped: %s", stream.path, err)
			})
			return err
		}

		r.lock.Lock()
		for id, sub := range stream.subscribers {
			if r.offerLocked(stream, id, sub, p) {
				// The subscriber is persistently behind. Drop it without
				// blocking the publisher or the other subscribers.
				delete(stream.subscribers, id)
				r.kickSubscriberLocked(stream.path, id, sub)
			}
		}
		r.lock.Unlock()

		p.Decommission()
	}
}

// offerLocked hands a packet to a subscriber. If the subscriber's queue is
// full, the oldest queued packet is dropped in favor of the new one: for a
// live stream a gap is better than a growing delay, and the publisher must
// never block on a slow reader. It returns true if the subscriber is
// hopelessly behind and should be kicked. The router lock must be held.
func (r *Router) offerLocked(stream *routerStream, id uint32, sub *routerSubscriber, p packet.Packet) bool {
	select {
	case sub.queue <- p.Clone():
		sub.drops = 0
		return false
	default:
	}

	// The queue is full. Drop the oldest packet and enqueue the new one.
	select {
	case old := <-sub.queue:
		old.Decommission()
	default:
	}

	select {
	case sub.queue <- p.Clone():
	default:
	}

	sub.drops++

	// Kick the subscriber once it has fallen behind by more than twice its
	// queue size without a single clean send in between.
	return sub.drops > 2*cap(sub.queue)
}

func (r *Router) serveSubscriber(route *Route, conn Conn) error {
	defer route.Release()

	stream := route.stream
	sub := route.sub

	r.lock.Lock()
	sub.conn = conn
	kicked := sub.kicked
	r.lock.Unlock()

	if kicked {
		conn.Close()
		return ErrSubscriberTooSlow
	}

	// Watch for the connection closing (peer shutdown, idle timeout) so
	// the subscriber slot is released promptly even if no data flows.
	var connDone <-chan struct{}
	if dc, ok := conn.(interface{ Done() <-chan struct{} }); ok {
		connDone = dc.Done()
	}

	for {
		select {
		case <-stream.ctx.Done():
			// The publisher disconnected.
			return io.EOF
		case <-sub.done:
			return ErrSubscriberTooSlow
		case <-connDone:
			return fmt.Errorf("subscriber on path %s disconnected", stream.path)
		case p := <-sub.queue:
			err := conn.WritePacket(p)
			p.Decommission()
			if err != nil {
				r.lock.Lock()
				kicked := sub.kicked
				r.lock.Unlock()

				if kicked {
					// The write failed because the subscriber
					// has been dropped for being too slow.
					return ErrSubscriberTooSlow
				}

				r.log("router:subscribe", func() string {
					return fmt.Sprintf("subscriber on path %s stopped: %s", stream.path, err)
				})
				return err
			}
		}
	}
}

// kickSubscriberLocked drops a slow subscriber. The router lock must be held.
func (r *Router) kickSubscriberLocked(path string, id uint32, sub *routerSubscriber) {
	if sub.kicked {
		return
	}
	sub.kicked = true
	close(sub.done)

	conn := sub.conn
	if conn != nil {
		// Close the connection in the background to unblock a subscriber
		// that is currently stuck in WritePacket.
		go conn.Close()
	}

	r.log("router:subscribe", func() string {
		return fmt.Sprintf("dropping slow subscriber on path %s", path)
	})
}

// Release returns the reserved slot of a route that will not be served.
// It is a no-op if the route has already been released or served.
func (route *Route) Release() {
	route.releaseOnce.Do(func() {
		r := route.router

		r.lock.Lock()
		defer r.lock.Unlock()

		stream := route.stream

		if route.Mode == PUBLISH {
			stream.publisher = false
			// End the stream: all subscribers of this path get io.EOF.
			stream.cancel()
			if r.streams[stream.path] == stream {
				delete(r.streams, stream.path)
			}
		} else {
			delete(stream.subscribers, route.subId)
			if !stream.publisher && len(stream.subscribers) == 0 {
				stream.cancel()
				if r.streams[stream.path] == stream {
					delete(r.streams, stream.path)
				}
			}
		}
	})
}

// Close shuts the router down. All routes are terminated and further
// Admit calls fail with ErrRouterClosed. Connections already accepted
// are not closed; their Serve calls return.
func (r *Router) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.closed = true

	for path, stream := range r.streams {
		stream.cancel()
		delete(r.streams, path)
	}
}

// SubscriberCount returns the current number of subscribers on a path.
func (r *Router) SubscriberCount(path string) int {
	r.lock.Lock()
	defer r.lock.Unlock()

	stream, ok := r.streams[path]
	if !ok {
		return 0
	}

	return len(stream.subscribers)
}

// HasPublisher returns whether the path currently has a publisher.
func (r *Router) HasPublisher(path string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	stream, ok := r.streams[path]
	if !ok {
		return false
	}

	return stream.publisher
}

func (r *Router) log(topic string, message func() string) {
	if r.logger == nil {
		return
	}

	r.logger.Print(topic, 0, 2, message)
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}

	return addr.String()
}
