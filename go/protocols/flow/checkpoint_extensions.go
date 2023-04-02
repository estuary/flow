package flow

import (
	jsonpatch "github.com/evanphx/json-patch/v5"
	pb "go.gazette.dev/core/broker/protocol"
)

// Validate returns an error if the DriverCheckpoint is malformed.
func (c *ConnectorState) Validate() error {
	if len(c.UpdatedJson) == 0 && c.MergePatch {
		return pb.NewValidationError("UpdatedJson cannot be empty if MergePatch is set")
	}
	return nil
}

// Reduce the other DriverCheckpoint into this one.
// Reduce is associative: (a.Reduce(b)).Reduce(c) equals a.Reduce(b.Reduce(c)).
func (c *ConnectorState) Reduce(other ConnectorState) error {
	// If |other| is not a patch we simply take its value.
	if !other.MergePatch {
		c.UpdatedJson = other.UpdatedJson
		c.MergePatch = false
		return nil
	}

	var err error
	if c.MergePatch {
		c.UpdatedJson, err = jsonpatch.MergeMergePatches(c.UpdatedJson, other.UpdatedJson)
	} else if len(c.UpdatedJson) != 0 {
		c.UpdatedJson, err = jsonpatch.MergePatch(c.UpdatedJson, other.UpdatedJson)
	} else {
		c.UpdatedJson, err = jsonpatch.MergePatch([]byte("{}"), other.UpdatedJson)
	}

	return err
}
