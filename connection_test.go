package srt

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryption(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:6003",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	{
		// Reject connection if wrong password is set
		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "barfoobarfoo"

		_, err := Dial("srt", "127.0.0.1:6003", config)
		require.Error(t, err)
	}
	// Test transmitting an encrypted message

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		n, err := conn.Write([]byte(message))
		if !assert.NoError(t, err) {
			panic(err.Error())
		}
		assert.Equal(t, 12, n)

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message, reader1)
}

// Test for https://github.com/datarhei/gosrt/pull/94
func TestEncryptionRetransmit(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:6003",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	{
		// Reject connection if wrong password is set
		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "barfoobarfoo"

		_, err := Dial("srt", "127.0.0.1:6003", config)
		require.Error(t, err)
	}

	// Test transmitting an encrypted message

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		counter := 0

		dialer, _ := conn.(*dialer)
		originalOnSend := dialer.conn.onSend
		dialer.conn.onSend = func(p packet.Packet) {
			if !p.Header().IsControlPacket {
				// Drop every 2nd original packet
				if !p.Header().RetransmittedPacketFlag {
					counter++
					if counter%2 == 0 {
						return
					}
				}
			}

			originalOnSend(p)
		}

		for range 5 {
			n, err := conn.Write([]byte(message))
			if !assert.NoError(t, err) {
				panic(err.Error())
			}
			assert.Equal(t, 12, n)
		}

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message+message+message+message+message, reader1)
}

func TestEncryptionKeySwap(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:6003",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	// Test transmitting encrypted messages with key swap in between

	dataReader1 := bytes.Buffer{}

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		buffer := make([]byte, 2048)

		close(readerConnected)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"
		// Swap encryption key after 50 sent messages
		config.KMPreAnnounce = 10
		config.KMRefreshRate = 30

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		// Send 150 messages
		for range 150 {
			n, err := conn.Write([]byte(message))
			if !assert.NoError(t, err) {
				panic(err.Error())
			}
			assert.Equal(t, 12, n)
		}

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, strings.Repeat(message, 150), reader1)
}

func TestStats(t *testing.T) {
	message := "Hello World!"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()

	server := Server{
		Addr:   "127.0.0.1:6003",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	statsReader := Statistics{}
	statsWriter := Statistics{}

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		conn.Stats(&statsReader)

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		n, err := conn.Write([]byte(message))
		if !assert.NoError(t, err) {
			panic(err.Error())
		}
		assert.Equal(t, 12, n)

		time.Sleep(3 * time.Second)

		conn.Stats(&statsWriter)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message, reader1)

	require.Equal(t, uint64(len(message)+44), statsReader.Accumulated.ByteRecv)
	require.Equal(t, uint64(1), statsReader.Accumulated.PktRecv)

	require.Equal(t, uint64(len(message)+44), statsWriter.Accumulated.ByteSent)
	require.Equal(t, uint64(1), statsWriter.Accumulated.PktSent)
}

// TestStatsGapRecovery sends data over a real connection while dropping the
// first original data packet on the publisher's send path. The NAK retransmission
// recovers it in time at the receiving (server side) connection, so the single
// gap must be reported as one recovered reordering rather than as a loss. The
// interval counters must be consistent with the accumulated counters.
func TestStatsGapRecovery(t *testing.T) {
	message := "Hello World!"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()

	// The receiving end of the publishing connection is where the dropped packet
	// shows up as a gap and is then recovered by the retransmission.
	pubStats := Statistics{}
	pubStatsReady := make(chan struct{})

	server := Server{
		Addr:   "127.0.0.1:6003",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Stats(&pubStats)
			close(pubStatsReady)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		require.NoError(t, conn.Close())
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"

		conn, err := Dial("srt", "127.0.0.1:6003", config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		dialer, _ := conn.(*dialer)
		originalOnSend := dialer.conn.onSend
		dropped := false

		// Drop the first original data packet exactly once; its retransmission
		// must recover the gap in time.
		dialer.conn.onSend = func(p packet.Packet) {
			if !dropped && !p.Header().IsControlPacket && !p.Header().RetransmittedPacketFlag {
				dropped = true
				return
			}

			originalOnSend(p)
		}

		for range 10 {
			n, err := conn.Write([]byte(message))
			if !assert.NoError(t, err) {
				panic(err.Error())
			}
			assert.Equal(t, 12, n)
		}

		time.Sleep(3 * time.Second)

		assert.NoError(t, conn.Close())
	}()

	<-writerDone
	<-readerDone
	<-pubStatsReady

	// Every message is recovered and delivered in order.
	require.Equal(t, strings.Repeat(message, 10), dataReader.String())

	acc := pubStats.Accumulated
	interval := pubStats.Interval

	// The single gap was a reordering recovered by retransmission, not a loss.
	require.Equal(t, uint64(0), acc.PktRecvLoss, "a recovered gap must not remain a loss")
	require.Equal(t, uint64(1), acc.PktRecvReordered, "the one gap is counted as a recovered reordering")
	require.Equal(t, uint64(0), acc.PktRecvBelated)
	require.Equal(t, uint64(0), acc.PktRecvDrop)
	require.Equal(t, uint64(10), acc.PktRecvUnique)
	require.GreaterOrEqual(t, acc.PktRecvRetrans, uint64(1), "the normal retransmission path is retained")

	// Interval counters are derived from the accumulated counters; with no prior
	// Stats snapshot the whole lifetime is one interval, so they must match.
	require.Equal(t, acc.PktRecvLoss, interval.PktRecvLoss)
	require.Equal(t, acc.PktRecvReordered, interval.PktRecvReordered)
	require.Equal(t, acc.PktRecvBelated, interval.PktRecvBelated)
	require.Equal(t, acc.PktRecvDrop, interval.PktRecvDrop)
	require.Equal(t, acc.ByteRecvLoss, interval.ByteRecvLoss)
	require.Equal(t, acc.ByteRecvReordered, interval.ByteRecvReordered)
	require.Equal(t, acc.ByteRecvBelated, interval.ByteRecvBelated)
}
