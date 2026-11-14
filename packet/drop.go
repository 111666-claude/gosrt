package packet

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/datarhei/gosrt/circular"
)

// 3.2.6.  Drop Request

// CIFDropRequest represents a Drop Request message. It is sent from the
// sender to the receiver in order to inform about a range of packets that
// the sender gave up on and will never (re)send, e.g. because they became
// too old (TLPKTDROP). The receiver must not request the retransmission of
// these packets anymore and skip them for delivery and acknowledgement.
type CIFDropRequest struct {
	MessageNumber             uint32
	FirstPacketSequenceNumber circular.Number
	LastPacketSequenceNumber  circular.Number
}

func (c CIFDropRequest) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "--- DROPREQ ---\n")
	fmt.Fprintf(&b, "   messageNumber: %#08x\n", c.MessageNumber)
	fmt.Fprintf(&b, "   firstPacketSequenceNumber: %#08x (%d)\n", c.FirstPacketSequenceNumber.Val(), c.FirstPacketSequenceNumber.Val())
	fmt.Fprintf(&b, "   lastPacketSequenceNumber: %#08x (%d)\n", c.LastPacketSequenceNumber.Val(), c.LastPacketSequenceNumber.Val())
	fmt.Fprintf(&b, "--- /DROPREQ ---")

	return b.String()
}

func (c *CIFDropRequest) Unmarshal(data []byte) error {
	if len(data) < 12 {
		return fmt.Errorf("data too short to unmarshal")
	}

	c.MessageNumber = binary.BigEndian.Uint32(data[0:])
	c.FirstPacketSequenceNumber = circular.New(binary.BigEndian.Uint32(data[4:])&MAX_SEQUENCENUMBER, MAX_SEQUENCENUMBER)
	c.LastPacketSequenceNumber = circular.New(binary.BigEndian.Uint32(data[8:])&MAX_SEQUENCENUMBER, MAX_SEQUENCENUMBER)

	return nil
}

func (c *CIFDropRequest) Marshal(w io.Writer) error {
	if w == nil {
		return fmt.Errorf("invalid writer")
	}

	var buffer [12]byte

	binary.BigEndian.PutUint32(buffer[0:], c.MessageNumber)
	binary.BigEndian.PutUint32(buffer[4:], c.FirstPacketSequenceNumber.Val())
	binary.BigEndian.PutUint32(buffer[8:], c.LastPacketSequenceNumber.Val())

	_, err := w.Write(buffer[0:])

	return err
}
