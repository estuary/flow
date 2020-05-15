package consumer

import (
	"bufio"
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// RawJSONMeta is metadata for the behavior of RawJSON message values.
type RawJSONMeta struct {
	UUIDPath    []string
	ACKTemplate []byte
}

// RawJSONMessage wraps a json.RawMessage to implement the message.Message,
// json.Unmarshaler, and message.JSONMarshalerTo interfaces.
type RawJSONMessage struct {
	json.RawMessage
	Meta *RawJSONMeta
}

var _ message.Message = (*RawJSONMessage)(nil)
var _ message.JSONMarshalerTo = (*RawJSONMessage)(nil)
var _ json.Unmarshaler = (*RawJSONMessage)(nil)

// GetUUID fetchs the UUID of the RawJSON message.
func (r RawJSONMessage) GetUUID() message.UUID {
	if val, err := findUUID(r.RawMessage, r.Meta.UUIDPath); err != nil {
		logrus.WithField("err", err).Error("failed to locate message UUID")
	} else if out, err := uuid.ParseBytes(val); err != nil {
		logrus.WithFields(logrus.Fields{"err": err, "val": val}).Error("failed to parse message UUID")
	} else {
		return out
	}
	return uuid.Nil
}

// SetUUID sets the value of the (pre-allocated) UUID within the RawJSON message.
func (r RawJSONMessage) SetUUID(uuid message.UUID) {
	if val, err := findUUID(r.RawMessage, r.Meta.UUIDPath); err != nil {
		logrus.WithField("err", err).Error("failed to locate message UUID")
	} else {
		copy(val, uuid.String()[:])
	}
}

// NewAcknowledgement builds and returns an initialized RawJSON message having
// a placeholder UUID.
func (r RawJSONMessage) NewAcknowledgement(pb.Journal) message.Message {
	var m = &RawJSONMessage{Meta: r.Meta}
	m.UnmarshalJSON(r.Meta.ACKTemplate)
	return m
}

// MarshalJSONTo marshals a RawJSON message with a following newline.
func (r RawJSONMessage) MarshalJSONTo(bw *bufio.Writer) (int, error) {
	return bw.Write(r.RawMessage)
}

func findUUID(bytes []byte, uuidPath []string) ([]byte, error) {
	var val, typ, _, err = jsonparser.Get(bytes, uuidPath...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch UUID: %w", err)
	} else if typ != jsonparser.String || len(val) != len(placeholderUUID) {
		return nil, fmt.Errorf("message UUID format is invalid: %v", val)
	}
	return val, nil
}

const placeholderUUID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
