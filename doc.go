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

For setups with a primary and one or more backup SRT entry points,
DialCandidates attempts several addresses and returns the first connection
that completed the SRT handshake. Later candidates are attempted after an
interval, so a primary that silently drops packets does not delay the
startup:

	conn, err := srt.DialCandidates("srt", []string{
		"primary.example.com:6000",
		"backup.example.com:6000",
	}, srt.DefaultConfig(), 250*time.Millisecond)
	if err != nil {
		// handle error
	}

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
*/
package srt
