package recoverylog

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// Validate validates a FSMHints instance.
func (h *FSMHints) Validate() error {
	for _, node := range h.LiveNodes {
		for _, segment := range node.Segments {
			if segment.Log == "" {
				// FSMHints.Log is implied value of segments without Log.
				segment.Log = h.Log // Temporarily set (note this a stack copy!).
			} else if segment.Log == h.Log {
				// Segments must not repeat the already-implied FSMHints.Log.
				return pb.NewValidationError("contained segment.Log duplicates FSMHints.Log")
			}

			if err := segment.Validate(); err != nil {
				return pb.ExtendContext(err, "Segment")
			}
		}
	}
	return nil
}
