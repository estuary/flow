package flow

import (
	jsonpatch "github.com/evanphx/json-patch/v5"
	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the DriverCheckpoint is malformed.
func (c *DriverCheckpoint) Validate() error {
	if len(c.DriverCheckpointJson) == 0 && c.Rfc7396MergePatch {
		return pb.NewValidationError("DriverCheckpointJson cannot be empty if Rfc7396MergePatch")
	}
	return nil
}

// Reduce the other DriverCheckpoint into this one.
// Reduce is associative: (a.Reduce(b)).Reduce(c) equals a.Reduce(b.Reduce(c)).
func (c *DriverCheckpoint) Reduce(other DriverCheckpoint) error {
	// If |other| is not a patch we simply take its value.
	if !other.Rfc7396MergePatch {
		c.DriverCheckpointJson = other.DriverCheckpointJson
		c.Rfc7396MergePatch = false
		return nil
	}

	var err error
	if c.Rfc7396MergePatch {
		c.DriverCheckpointJson, err = jsonpatch.MergeMergePatches(
			c.DriverCheckpointJson, other.DriverCheckpointJson)
	} else if len(c.DriverCheckpointJson) != 0 {
		c.DriverCheckpointJson, err = jsonpatch.MergePatch(
			c.DriverCheckpointJson, other.DriverCheckpointJson)
	} else {
		c.DriverCheckpointJson, err = jsonpatch.MergePatch(
			[]byte("{}"), other.DriverCheckpointJson)
	}

	return err
}
