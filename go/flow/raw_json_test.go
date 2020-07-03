package flow

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
)

func TestRawJSONMessageCases(t *testing.T) {
	var ackTemplate = `{"path":{"to":{"uuid":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}}, "ack": true}`

	var spec = brokertest.Journal(pb.JournalSpec{
		Name: "foo/bar",
		LabelSet: pb.MustLabelSet(
			labels.UUIDPointer, "/path/to/uuid",
			labels.ACKTemplate, base64.StdEncoding.EncodeToString([]byte(ackTemplate)),
		),
	})
	require.NoError(t, spec.Validate())

	// Case: Successful round-trips of a message and acknowledgment.
	msg, err := NewRawJSONMessage(spec)
	require.NoError(t, err)

	// Initially, the UUID is zero.
	require.Equal(t, uuid.UUID{}, msg.GetUUID())

	// We can unmarshal the message, and fetch it's UUID.
	var msgUUID = uuid.MustParse("000001a8-0000-1000-9402-000102030405")
	var msgFixture = `{"path":{"to":{"uuid":"` + msgUUID.String() + `"}}, "message": true}`
	require.NoError(t, msg.(*RawJSONMessage).UnmarshalJSON([]byte(msgFixture)))
	require.Equal(t, msgUUID, msg.GetUUID())
	// The message round-trips back to its serialization.
	require.Equal(t, msgFixture, rawToString(t, msg.(*RawJSONMessage)))

	// We can build a message acknowledgement, and set its UUID.
	var ack = msg.NewAcknowledgement("other/journal")
	var ackUUID = uuid.MustParse("000001a8-0000-1000-9402-000102030405")
	ack.SetUUID(ackUUID)
	// We can re-read the UUID fixture just set.
	require.Equal(t, ackUUID, ack.GetUUID())
	// It has the expected marshalled bytes (ACK template, updated with |aUUID|).
	require.Equal(t, `{"path":{"to":{"uuid":"`+ackUUID.String()+`"}}, "ack": true}`,
		rawToString(t, ack.(*RawJSONMessage)))

	// Case: Attempt to set a non-v1 UUID panics.
	var v4UUID = uuid.MustParse("9b89dd92-b5d3-48cc-bdf5-5bf8063ab853")
	require.PanicsWithValue(t, "not a RFC 4122 v1 UUID", func() { msg.SetUUID(v4UUID) })

	// Case: message doesn't contain UUID at expected path.
	require.EqualError(t,
		msg.(*RawJSONMessage).UnmarshalJSON([]byte(`{"missing-uuid": true}`)),
		"failed to locate UUID within RawJSONMessage: failed to fetch UUID: Key path not found")
	// Case: message UUID is malformed.
	require.EqualError(t, msg.(*RawJSONMessage).UnmarshalJSON(
		[]byte(`{"path":{"to":{"uuid":"0000-111-222-33"}}}`)),
		"failed to locate UUID within RawJSONMessage: message UUID format is invalid: 0000-111-222-33")
	// Case: message UUID is wrong type.
	require.EqualError(t, msg.(*RawJSONMessage).UnmarshalJSON(
		[]byte(`{"path":{"to":{"uuid":[true]}}}`)),
		"failed to locate UUID within RawJSONMessage: message UUID format is invalid: [true]")
	// Case: message is not JSON
	require.EqualError(t, msg.(*RawJSONMessage).UnmarshalJSON(
		[]byte(`{"path":{"to":{"uuid":`)),
		"failed to locate UUID within RawJSONMessage: failed to fetch UUID: Malformed JSON error")
	// Case: message UUID is right shape, but invalid.
	require.EqualError(t, msg.(*RawJSONMessage).UnmarshalJSON(
		[]byte(`{"path":{"to":{"uuid":"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}}}`)),
		"failed to parse UUID xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx: invalid UUID format")
	// Case: message UUID is valid but not V1.
	require.EqualError(t, msg.(*RawJSONMessage).UnmarshalJSON(
		[]byte(`{"path":{"to":{"uuid":"`+v4UUID.String()+`"}}}`)),
		v4UUID.String()+" is not a RFC 4122 v1 UUID (it's version VERSION_4)")

	// Allocate a new JournalSpec to force re-initialization of RawJSONMeta
	var badSpec = *spec

	// Case: invalid UUID pointer.
	badSpec.LabelSet.SetValue(labels.UUIDPointer, "bad json ptr")
	_, err = NewRawJSONMessage(&badSpec)
	require.EqualError(t, err, "NewRawJSONMeta: invalid UUID pointer: bad json ptr")
	badSpec.LabelSet.SetValue(labels.UUIDPointer, "/ok/ptr")

	// Case: non-base64 ACK template.
	badSpec.LabelSet.SetValue(labels.ACKTemplate, "not base64")
	_, err = NewRawJSONMessage(&badSpec)
	require.EqualError(t, err,
		"NewRawJSONMeta: failed to base64-decode ACK template: illegal base64 data at input byte 3")

	// Case: ACK template doesn't have a valid placeholder UUID.
	var badTemplate = `{"path":{"to":{"uuid":"wrong-shape"}}, "ack": true}`
	badSpec.LabelSet.SetValue(labels.ACKTemplate, base64.StdEncoding.EncodeToString([]byte(badTemplate)))
	_, err = NewRawJSONMessage(&badSpec)
	require.EqualError(t, err,
		"NewRawJSONMeta: invalid ACK template: failed to fetch UUID: Key path not found")
}

func rawToString(t *testing.T, m *RawJSONMessage) string {
	var b bytes.Buffer
	var bw = bufio.NewWriter(&b)
	var _, err = m.MarshalJSONTo(bw)
	require.NoError(t, err)
	bw.Flush()

	require.Equal(t, []byte(m.Raw), b.Bytes())
	return b.String()
}
