package capture

import (
	"encoding/json"
	"fmt"

	pf "github.com/estuary/protocols/flow"
)

// StageCaptured potentially sends a previously staged Captured into the stream,
// and then stages its arguments into response.Captured.
func StageCaptured(
	stream interface {
		Send(*CaptureResponse) error
	},
	response **CaptureResponse,
	binding int,
	document json.RawMessage,
) error {
	// Send current |response| if we would re-allocate.
	if *response != nil {
		var rem int
		if l := (*response).Captured; int(l.Binding) != binding {
			rem = -1 // Must flush this response.
		} else if cap(l.DocsJson) != len(l.DocsJson) {
			rem = cap(l.Arena) - len(l.Arena)
		}
		if rem < len(document) {
			if err := stream.Send(*response); err != nil {
				return fmt.Errorf("sending Captured response: %w", err)
			}
			*response = nil
		}
	}

	if *response == nil {
		*response = &CaptureResponse{
			Captured: &CaptureResponse_Captured{
				Binding:  uint32(binding),
				Arena:    make(pf.Arena, 0, arenaSize),
				DocsJson: make([]pf.Slice, 0, sliceSize),
			},
		}
	}

	var l = (*response).Captured
	l.DocsJson = append(l.DocsJson, l.Arena.Add(document))

	return nil
}

// WriteCommit flushes a pending Captured response, and sends a Commit response
// with the provided driver checkpoint.
func WriteCommit(
	stream interface {
		Send(*CaptureResponse) error
	},
	response **CaptureResponse,
	commit *CaptureResponse_Commit,
) error {
	// Flush partial Captured response, if required.
	if *response != nil {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("flushing final Captured response: %w", err)
		}
		*response = nil
	}

	if err := stream.Send(&CaptureResponse{
		Commit: commit,
	}); err != nil {
		return fmt.Errorf("sending Commit response: %w", err)
	}

	return nil
}

const (
	arenaSize = 16 * 1024
	sliceSize = 32
)
