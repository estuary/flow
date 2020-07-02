package flow

import (
	"encoding/base64"
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
)

func TestRawJSONMessageRoundTrips(t *testing.T) {

	var aUUID = uuid.MustParse("000001a8-0000-1000-9402-000102030405")
	var ackTemplate = `{"path":{"to":{"uuid":"` + placeholderUUID + `"}}, "other": true}`

	var spec = brokertest.Journal(pb.JournalSpec{
		Name: "foo/bar",
		LabelSet: pb.MustLabelSet(
			labels.UUIDPointer, "/path/to/uuid",
			labels.ACKTemplate, base64.RawStdEncoding.EncodeToString([]byte(ackTemplate)),
		),
	})
	require.NoError(t, spec.Validate())

	var msg, err = NewRawJSONMessage(spec)
	require.NoError(t, err)






	// We can build a message acknowledgement, and set its UUID.
	var ack = msg.NewAcknowledgement("other/journal")
	ack.SetUUID(aUUID)

	// It has the expected marshalled bytes (ACK template set with |aUUID|).
	b, err := ack.(*RawJSONMessage).MarshalJSON()
	require.NoError(t, err)
	require.Equal(t,
		`{"path":{"to":{"uuid":"000001a8-0000-1000-9402-000102030405"}}, "other": true}`,
		string(b))

	// We can re-read the UUID fixture just set.
	require.Equal(t, aUUID, ack.GetUUID())
}
