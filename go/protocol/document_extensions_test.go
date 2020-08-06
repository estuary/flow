package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

func TestArena(t *testing.T) {
	var a Arena
	require.Equal(t, []byte{4, 2, 7}, a.Bytes(a.Add([]byte{4, 2, 7})))

	var fixture = [][]byte{[]byte("foo!"), []byte("bar\n"), []byte("qip")}
	var slices = a.AddAll(fixture...)
	require.Equal(t, fixture, a.AllBytes(slices...))
}

func TestUUIDPartsRoundTrip(t *testing.T) {
	var producer = message.ProducerID{8, 6, 7, 5, 3, 9}

	var clock message.Clock
	clock.Update(time.Unix(1594821664, 47589100)) // Timestamp resolution is 100ns.
	clock.Tick()                                  // Further ticks of sequence bits.
	clock.Tick()

	var parts = NewUUIDParts(message.BuildUUID(producer, clock, message.Flag_ACK_TXN))
	require.Equal(t, UUIDParts{
		ProducerAndFlags: 0x0806070503090000 + 0x02, // Producer + flags.
		Clock:            0x1eac6a39f2952f32,
	}, parts)

	var uuid = parts.Pack()
	require.Equal(t, "9f2952f3-c6a3-11ea-8802-080607050309", uuid.String())
	require.Equal(t, message.GetProducerID(uuid), producer)
	require.Equal(t, message.GetFlags(uuid), message.Flag_ACK_TXN)
	require.Equal(t, message.GetClock(uuid), clock)
}

/*
// TODO(johnny): Rework as part of DeriveMessage.

func TestDocumentJSONInterface(t *testing.T) {
	// Documents can build acknowledgements.
	var doc = new(Document).NewAcknowledgement("").(*Document)
	require.Equal(t, doc.Content, []byte(DocumentAckJSONTemplate))
	require.Equal(t, doc.ContentType, Document_JSON)

	// We can update the UUID.
	var testUUID = uuid.MustParse("000001a8-0000-1000-9402-000102030405")
	doc.SetUUID(testUUID)

	// The updated UUID is seen via accessor, and serialization.
	require.Equal(t, testUUID, doc.GetUUID())
	var serialization = documentToString(t, doc)
	require.Equal(t, `{"_meta":{"uuid":"000001a8-0000-1000-9402-000102030405","ack":true}}`+"\n",
		serialization)

	// We can round-trip back to a Document (though UUID is parsed elsewhere).
	require.NoError(t, doc.UnmarshalJSON([]byte(serialization)))
	require.Equal(t, doc.Content, []byte(serialization))
	require.Equal(t, doc.ContentType, Document_JSON)
	require.Equal(t, serialization, documentToString(t, doc))
}

func documentToString(t *testing.T, m *Document) string {
	var b bytes.Buffer
	var bw = bufio.NewWriter(&b)
	var _, err = m.MarshalJSONTo(bw)
	require.NoError(t, err)
	bw.Flush()

	require.Equal(t, []byte(m.Content), b.Bytes())
	return b.String()
}

*/
