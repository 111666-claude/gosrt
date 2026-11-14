package packet

import (
	"bytes"
	"testing"

	"github.com/datarhei/gosrt/circular"
	"github.com/stretchr/testify/require"
)

func TestCIFDropRequestString(t *testing.T) {
	cif := &CIFDropRequest{
		MessageNumber:             7,
		FirstPacketSequenceNumber: circular.New(102, MAX_SEQUENCENUMBER),
		LastPacketSequenceNumber:  circular.New(103, MAX_SEQUENCENUMBER),
	}

	require.Equal(t, "--- DROPREQ ---\n   messageNumber: 0x00000007\n   firstPacketSequenceNumber: 0x00000066 (102)\n   lastPacketSequenceNumber: 0x00000067 (103)\n--- /DROPREQ ---", cif.String())
}

func TestCIFDropRequestMarshalUnmarshal(t *testing.T) {
	cif := &CIFDropRequest{
		MessageNumber:             42,
		FirstPacketSequenceNumber: circular.New(102, MAX_SEQUENCENUMBER),
		LastPacketSequenceNumber:  circular.New(1101, MAX_SEQUENCENUMBER),
	}

	var buf bytes.Buffer
	require.NoError(t, cif.Marshal(&buf))
	require.Equal(t, 12, buf.Len())

	cif2 := &CIFDropRequest{}
	require.NoError(t, cif2.Unmarshal(buf.Bytes()))

	require.Equal(t, uint32(42), cif2.MessageNumber)
	require.Equal(t, uint32(102), cif2.FirstPacketSequenceNumber.Val())
	require.Equal(t, uint32(1101), cif2.LastPacketSequenceNumber.Val())
}

func TestCIFDropRequestMarshalUnmarshalBoundary(t *testing.T) {
	// Sequence numbers at and around the wrap-around point
	cif := &CIFDropRequest{
		MessageNumber:             1,
		FirstPacketSequenceNumber: circular.New(MAX_SEQUENCENUMBER, MAX_SEQUENCENUMBER),
		LastPacketSequenceNumber:  circular.New(0, MAX_SEQUENCENUMBER),
	}

	var buf bytes.Buffer
	require.NoError(t, cif.Marshal(&buf))

	cif2 := &CIFDropRequest{}
	require.NoError(t, cif2.Unmarshal(buf.Bytes()))

	require.Equal(t, MAX_SEQUENCENUMBER, cif2.FirstPacketSequenceNumber.Val())
	require.Equal(t, uint32(0), cif2.LastPacketSequenceNumber.Val())
}

func TestCIFDropRequestUnmarshalInvalid(t *testing.T) {
	cif := &CIFDropRequest{}

	require.Error(t, cif.Unmarshal(nil))
	require.Error(t, cif.Unmarshal([]byte{1, 2, 3, 4}))
	require.Error(t, cif.Unmarshal(make([]byte, 11)))
}

func TestCIFDropRequestMarshalInvalidWriter(t *testing.T) {
	cif := &CIFDropRequest{}

	require.Error(t, cif.Marshal(nil))
}
