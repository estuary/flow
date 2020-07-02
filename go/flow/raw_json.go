package flow

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/buger/jsonparser"
	"github.com/estuary/flow/go/labels"
	"github.com/google/uuid"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// RawJSONMessage wraps a json.RawMessage to implement the message.Message,
// json.Unmarshaler, and message.JSONMarshalerTo interfaces.
type RawJSONMessage struct {
	Meta               *RawJSONMeta
	Raw                json.RawMessage
	UUID               message.UUID
	ShuffledTransforms []int64
}

// RawJSONMeta is metadata for the behavior of RawJSON message values.
// Instances are pooled & indexed, and are created on-demand from JournalSpec labels.
type RawJSONMeta struct {
	UUIDPath    []string
	ACKTemplate []byte
}

// NewRawJSONMeta returns a RawJSONMeta derived from the JournalSpec.
func NewRawJSONMeta(spec *pb.JournalSpec) (*RawJSONMeta, error) {
	// TODO(johnny): Handle `jsonparser`'s index "[0]" syntax.
	var uuidPtr = spec.LabelSet.ValueOf(labels.UUIDPointer)

	if len(uuidPtr) == 0 || uuidPtr[0] != '/' {
		return nil, fmt.Errorf("invalid UUID pointer: %s", uuidPtr)
	}

	var uuidPath = strings.Split(uuidPtr[1:], "/")
	var ackTemplate, err = base64.StdEncoding.DecodeString(
		spec.LabelSet.ValueOf(labels.ACKTemplate))

	if err != nil {
		return nil, fmt.Errorf("failed to base64-decode ACK template: %w", err)
	} else if _, err = findUUID(ackTemplate, uuidPath); err != nil {
		return nil, fmt.Errorf("invalid ACK template: %w", err)
	}

	return &RawJSONMeta{
		UUIDPath:    uuidPath,
		ACKTemplate: ackTemplate,
	}, nil
}

// RawJSONMessage is a Message, a JSONMarshalerTo, and Unmarshaler.
var _ message.Message = (*RawJSONMessage)(nil)
var _ message.JSONMarshalerTo = (*RawJSONMessage)(nil)
var _ json.Unmarshaler = (*RawJSONMessage)(nil)

// messageMetaPool is a pool of maps, each indexing *JournalSpecs to a corresponding
// *RawJSONMeta sync.Pool uses thread-specific pointers under the hood, and we pool
// un-synchronized maps to allow physical cores to run without contention.
var messageMetaPool = sync.Pool{
	New: func() interface{} {
		return make(map[*pb.JournalSpec]*RawJSONMeta)
	},
}

// NewRawJSONMessage returns an empty RawJSONMessage.
func NewRawJSONMessage(spec *pb.JournalSpec) (message.Message, error) {
	var m = messageMetaPool.Get().(map[*pb.JournalSpec]*RawJSONMeta)
	defer messageMetaPool.Put(m)

	if meta, ok := m[spec]; ok {
		return &RawJSONMessage{Meta: meta}, nil
	} else if meta, err := NewRawJSONMeta(spec); err != nil {
		return nil, fmt.Errorf("NewRawJSONMeta: %w", err)
	} else {
		m[spec] = meta
		return &RawJSONMessage{Meta: meta}, nil
	}
}

// GetUUID fetches the UUID of the RawJSON message.
func (r *RawJSONMessage) GetUUID() message.UUID { return r.UUID }

// SetUUID sets the value of the (pre-allocated) UUID within the RawJSON message.
func (r *RawJSONMessage) SetUUID(uuid message.UUID) {
	if val, err := findUUID(r.Raw, r.Meta.UUIDPath); err != nil {
		panic(err) // Already checked by UnmarshalJSON or NewAcknowledgement.
	} else {
		copy(val, uuid.String()[:])
	}
}

// NewAcknowledgement builds and returns an initialized RawJSON message having
// a placeholder UUID.
func (r *RawJSONMessage) NewAcknowledgement(pb.Journal) message.Message {
	var m = &RawJSONMessage{Meta: r.Meta}
	if err := r.Raw.UnmarshalJSON(r.Meta.ACKTemplate); err != nil {
		panic(err) // Fails only if Raw is nil.
	}
	return m
}

// MarshalJSONTo marshals a RawJSON message with a following newline.
func (r *RawJSONMessage) MarshalJSONTo(bw *bufio.Writer) (int, error) {
	return bw.Write(r.Raw)
}

// UnmarshalJSON initializes the RawJSONMessage from the given bytes,
// and verifies that it has a set UUID.
func (r *RawJSONMessage) UnmarshalJSON(data []byte) error {
	if err := r.Raw.UnmarshalJSON(data); err != nil {
		panic(err) // Fails only if Raw is nil.
	} else if val, err := findUUID(r.Raw, r.Meta.UUIDPath); err != nil {
		return fmt.Errorf("failed to locate UUID within RawJSONMessage: %w", err)
	} else if r.UUID, err = uuid.ParseBytes(val); err != nil {
		return fmt.Errorf("failed to parse UUID: %w", err)
	} else if v := r.UUID.Version(); v != 1 {
		return fmt.Errorf("%s is not a RFC 4122 v1 UUID (it's version %s)", r.UUID, v)
	}
	return nil
}

func findUUID(bytes []byte, uuidPath []string) ([]byte, error) {
	var val, typ, _, err = jsonparser.Get(bytes, uuidPath...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch UUID: %w", err)
	} else if typ != jsonparser.String || len(val) != 36 {
		return nil, fmt.Errorf("message UUID format is invalid: %v", val)
	}
	return val, nil
}
