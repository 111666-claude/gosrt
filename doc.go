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

# Structured streamid routing

For servers that receive multiple named streams on a single listener,
the Router type adds an optional routing layer on top of the Listener
API. It parses structured streamids (srt.ParseStreamId), enforces one
publisher per resource path plus a configurable subscriber quota during
the handshake, and fans the published data out to the subscribers of
the same path:

	router := srt.NewRouter(srt.RouterConfig{
		MaxSubscribersPerPath: 100,
	})

	for {
		req, err := ln.Accept2()
		if err != nil {
			// handle error
		}

		go func() {
			route, conn, err := router.Accept(req)
			if err != nil {
				// rejected during the handshake
				return
			}

			router.Serve(route, conn)
			conn.Close()
		}()
	}

Clients publish with a streamid like "#!::m=publish,r=live/match1" and
subscribe with "#!::m=request,r=live/match1". Credentials in streamids
never appear in the router's logs.
*/
package srt
