package srt

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"

	"github.com/stretchr/testify/require"
)

// sendHandshakePair drives one full caller-side SRT handshake over a raw UDP
// socket (induction request, induction response, conclusion request). The
// callerSocketId is the socket ID the caller advertises in its handshakes. The
// returned conclusion packet can be marshalled again to retransmit it, which is
// what a high latency caller does when it does not receive the response in time.
func sendHandshakePair(t *testing.T, conn net.Conn, callerSocketId uint32) (packet.Packet, *packet.CIFHandshake) {
	t.Helper()

	// send induction request
	p := packet.NewPacket(conn.RemoteAddr())
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
	p.Header().SubType = 0
	p.Header().TypeSpecific = 0
	p.Header().Timestamp = 0
	p.Header().DestinationSocketId = 0
	sendcif := &packet.CIFHandshake{
		IsRequest:                   true,
		Version:                     4,
		EncryptionField:             0,
		ExtensionField:              2,
		InitialPacketSequenceNumber: circular.New(10000, packet.MAX_SEQUENCENUMBER),
		MaxTransmissionUnitSize:     MAX_MSS_SIZE,
		MaxFlowWindowSize:           25600,
		HandshakeType:               packet.HSTYPE_INDUCTION,
		SRTSocketId:                 callerSocketId,
		SynCookie:                   0,
	}
	sendcif.PeerIP.FromNetAddr(conn.LocalAddr())
	p.MarshalCIF(sendcif)
	var buf bytes.Buffer
	err := p.Marshal(&buf)
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	// read induction response
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	inbuf := make([]byte, 1024)
	n, err := conn.Read(inbuf)
	require.NoError(t, err)
	p, err = packet.NewPacketFromData(conn.RemoteAddr(), inbuf[:n])
	require.NoError(t, err)
	recvcif := &packet.CIFHandshake{}
	err = p.UnmarshalCIF(recvcif)
	require.NoError(t, err)

	// send conclusion
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
	p.Header().SubType = 0
	p.Header().TypeSpecific = 0
	p.Header().Timestamp = 0
	p.Header().DestinationSocketId = 0
	sendcif.Version = 5
	sendcif.ExtensionField = recvcif.ExtensionField
	sendcif.HandshakeType = packet.HSTYPE_CONCLUSION
	sendcif.SynCookie = recvcif.SynCookie
	sendcif.HasHS = true
	sendcif.SRTHS = &packet.CIFHandshakeExtension{
		SRTVersion: SRT_VERSION,
		SRTFlags: packet.CIFHandshakeExtensionFlags{
			TSBPDSND:      true,
			TSBPDRCV:      true,
			CRYPT:         true,
			TLPKTDROP:     true,
			PERIODICNAK:   true,
			REXMITFLG:     true,
			STREAM:        false,
			PACKET_FILTER: false,
		},
		RecvTSBPDDelay: uint16(120),
		SendTSBPDDelay: uint16(120),
	}
	sendcif.HasSID = true
	sendcif.StreamId = "foobar"
	p.MarshalCIF(sendcif)
	buf.Reset()
	err = p.Marshal(&buf)
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	return p, sendcif
}
