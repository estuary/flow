package capture

import (
	"encoding/json"
	"fmt"
	"io"

	pf "github.com/estuary/protocols/flow"
)

// StagePullDocuments potentially sends a previously staged Documents into
// the stream, and then stages its arguments into response.Captured.
func StagePullDocuments(
	stream interface {
		Send(*PullResponse) error
	},
	response **PullResponse,
	binding int,
	document json.RawMessage,
) error {
	// Send current |response| if we would re-allocate.
	if *response != nil {
		var rem int
		if l := (*response).Documents; int(l.Binding) != binding {
			rem = -1 // Must flush this response.
		} else if cap(l.DocsJson) != len(l.DocsJson) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(document) {
			if err := stream.Send(*response); err != nil {
				return fmt.Errorf("sending Documents response: %w", err)
			}
			*response = nil
		}
	}

	if *response == nil {
		*response = &PullResponse{
			Documents: &Documents{
				Binding:  uint32(binding),
				Arena:    make(pf.Arena, 0, arenaSize),
				DocsJson: make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var l = (*response).Documents
	l.DocsJson = append(l.DocsJson, l.Arena.Add(document))

	return nil
}

// StagePushDocuments potentially sends a previously staged Documents into
// the stream, and then stages its arguments into response.Captured.
func StagePushDocuments(
	stream interface {
		Send(*PushRequest) error
	},
	request **PushRequest,
	binding int,
	document json.RawMessage,
) error {
	// Send current |request| if we would re-allocate.
	if *request != nil {
		var rem int
		if l := (*request).Documents; int(l.Binding) != binding {
			rem = -1 // Must flush this response.
		} else if cap(l.DocsJson) != len(l.DocsJson) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(document) {
			if err := stream.Send(*request); err != nil {
				return fmt.Errorf("sending Documents request: %w", err)
			}
			*request = nil
		}
	}

	if *request == nil {
		*request = &PushRequest{
			Documents: &Documents{
				Binding:  uint32(binding),
				Arena:    make(pf.Arena, 0, arenaSize),
				DocsJson: make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var l = (*request).Documents
	l.DocsJson = append(l.DocsJson, l.Arena.Add(document))

	return nil
}

// WritePullCheckpoint flushes a pending Documents response,
// and sends a Checkpoint response with the provided driver checkpoint.
func WritePullCheckpoint(
	stream interface {
		Send(*PullResponse) error
	},
	response **PullResponse,
	checkpoint *pf.DriverCheckpoint,
) error {
	// Flush partial Documents response, if required.
	if *response != nil {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("flushing final Documents response: %w", err)
		}
		*response = nil
	}

	if err := stream.Send(&PullResponse{Checkpoint: checkpoint}); err != nil {
		return fmt.Errorf("sending Checkpoint response: %w", err)
	}

	return nil
}

// WritePushCheckpoint flushes a pending Documents response,
// and sends a Checkpoint response with the provided driver checkpoint.
func WritePushCheckpoint(
	stream interface {
		Send(*PushRequest) error
	},
	request **PushRequest,
	checkpoint *pf.DriverCheckpoint,
) error {
	// Flush partial Documents request, if required.
	if *request != nil {
		if err := stream.Send(*request); err != nil {
			return fmt.Errorf("flushing final Documents request: %w", err)
		}
		*request = nil
	}

	if err := stream.Send(&PushRequest{Checkpoint: checkpoint}); err != nil {
		return fmt.Errorf("sending Checkpoint request: %w", err)
	}

	return nil
}

// ReadPushCheckpoint reads Documents from a Push RPC until a checkpoint
// is countered. It errors if more than |maxBytes| of Document byte content
// is read.
func ReadPushCheckpoint(
	stream interface {
		Recv() (*PushRequest, error)
	},
	maxBytes int,
) ([]Documents, pf.DriverCheckpoint, error) {

	var n int
	var docs []Documents

	for n < maxBytes {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF && len(docs) != 0 {
				err = io.ErrUnexpectedEOF
			}
			return nil, pf.DriverCheckpoint{}, err
		} else if err = req.Validate(); err != nil {
			return nil, pf.DriverCheckpoint{}, err
		}

		switch {
		case req.Documents != nil:
			docs = append(docs, *req.Documents)
			n += len(req.Documents.Arena)
		case req.Checkpoint != nil:
			return docs, *req.Checkpoint, nil
		}
	}

	return nil, pf.DriverCheckpoint{}, fmt.Errorf(
		"too many documents without a checkpoint (%d bytes vs max of %d)",
		n, maxBytes)
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
