/*
Package srt provides an interface for network I/O using the SRT protocol (https://github.com/Haivision/srt).

The package gives access to the basic interface provided by the Dial, Listen, and Accept functions and the associated
Conn and Listener interfaces.

The Dial function connects to a server:

	conn, err := srt.Dial("srt", "golang.org:6000", srt.Config{
		StreamId: "...",
	})
	if err != nil {
		// handle error
	}

	buffer := make([]byte, 2048)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			// handle error
		}

		// handle received data
	}

	conn.Close()

The Listen function creates servers:

	ln, err := srt.Listen("srt", ":6000", srt.Config{...})
	if err != nil {
		// handle error
	}

	for {
		conn, mode, err := ln.Accept(handleConnect)
		if err != nil {
			// handle error
		}

		if mode == srt.REJECT {
			// rejected connection, ignore
			continue
		}

		if mode == srt.PUBLISH {
			go handlePublish(conn)
		} else {
			go handleSubscribe(conn)
		}
	}

The ln.Accept function expects a function that takes a srt.ConnRequest
and returns a srt.ConnType. The srt.ConnRequest lets you retrieve the
streamid with on which you can decide what mode (srt.ConnType) to return.

Check out the Server type that wraps the Listen and Accept into a
convenient framework for your own SRT server.

# Periodic statistics

Conn.Stats fills a caller-provided Statistics value on demand. For live
monitoring, Conn.SubscribeStats samples the connection on a separate
goroutine and delivers snapshots on a bounded channel:

	sub, err := conn.SubscribeStats(srt.StatsSubscriptionConfig{
		Interval: time.Second,
		Buffer:   60,
	})
	if err != nil {
		// handle error
	}
	defer sub.Cancel()

	for s := range sub.C() {
		// s.Stats.Accumulated, s.Stats.Interval, s.Stats.Instantaneous
		// and s.SampledAt describe the sample.
	}

Each subscription has its own interval counters, buffer and timer, so
several consumers can sample the same connection independently. Sampling
never blocks the send or receive path; when a consumer is slower than the
sampling interval, the oldest queued snapshot is dropped. Cancel stops the
timer and closes the channel, and closing the connection terminates all
subscriptions.
*/
package srt
