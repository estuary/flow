package lifecycle

import (
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// StageCaptured potentially sends a previously staged Captured into the stream,
// and then stages its arguments into response.Captured.
func StageCaptured(
	stream interface {
		Send(*pc.CaptureResponse) error
	},
	response **pc.CaptureResponse,
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
		*response = &pc.CaptureResponse{
			Captured: &pc.CaptureResponse_Captured{
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
		Send(*pc.CaptureResponse) error
	},
	response **pc.CaptureResponse,
	commit *pc.CaptureResponse_Commit,
) error {
	// Flush partial Captured response, if required.
	if *response != nil {
		if err := stream.Send(*response); err != nil {
			return fmt.Errorf("flushing final Captured response: %w", err)
		}
		*response = nil
	}

	if err := stream.Send(&pc.CaptureResponse{
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
