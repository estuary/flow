package cloudstore

import (
	"errors"
	"fmt"
)

// GCSEndpoint is a fully-defined GCS endpoint with bucket and subfolder.
type GCSEndpoint struct {
	BaseEndpoint

	GCSBucket    string `json:"bucket"`
	GCSSubfolder string `json:"subfolder"`

	//TODO(Azim): Enable connecting to non-Arbor GCS accounts.
}

// Validate satisfies the model interface.
func (ep *GCSEndpoint) Validate() error {
	if ep.GCSBucket == "" {
		return errors.New("must specify gcs bucket")
	}
	return ep.BaseEndpoint.Validate()
}

// CheckPermissions satisfies the Endpoint interface.
func (ep *GCSEndpoint) CheckPermissions() error {
	return checkPermissions(ep, ep.PermissionTestFilename)
}

// Connect satisfies the Endpoint interface, returning a usable connection to the
// underlying GCS filesystem.
func (ep *GCSEndpoint) Connect(more Properties) (FileSystem, error) {
	var prop, err = mergeProperties(more, EmptyProperties())
	if err != nil {
		return nil, err
	}
	return NewFileSystem(prop, ep.uri())
}

func (ep *GCSEndpoint) uri() string {
	return fmt.Sprintf("gs://%s/%s", ep.GCSBucket, ep.GCSSubfolder)
}
