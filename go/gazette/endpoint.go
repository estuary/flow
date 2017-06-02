package cloudstore

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Endpoint reflects a common interface for structs with connection information
// to an arbitrary |FileSystem|.
type Endpoint interface {
	// CheckPermissions connects to the endpoint and confirms that the
	// passed credentials have read/write permissions in the root directory.
	CheckPermissions() error
	// Connect returns a FileSystem to be used by the caller, allowing the caller
	// to specify an arbitrary set of additional |Properties|. In most cases,
	// this will be unnecessary, as all connection details will be specified
	// by the Endpoint. |Properties| passed will be merged with those defined in the
	// Endpoint, overwriting the Endpoint properties where necessary.
	Connect(Properties) (FileSystem, error)
}

// UnmarshalEndpoint takes a byte array of json data (usually from etcd) and
// returns the appropriate |Endpoint| interface implementation.
func UnmarshalEndpoint(data []byte) (Endpoint, error) {
	var base BaseEndpoint
	if err := json.Unmarshal(data, &base); err != nil {
		return nil, err
	}
	switch base.Type {
	case "s3":
		var ep S3Endpoint
		if err := json.Unmarshal(data, &ep); err != nil {
			return nil, err
		}
		return &ep, nil
	case "sftp":
		var ep SFTPEndpoint
		if err := json.Unmarshal(data, &ep); err != nil {
			return nil, err
		}
		return &ep, nil
	default:
		panic(fmt.Sprintf("unknown endpoint type: %s", base.Type))
	}
}

// BaseEndpoint provides common fields for all endpoints. Though it currently
// only contains a |Name| field, it's important to maintain this inheritence
// to allow us to use |Name| as a primary key in the endpoint namespace.
type BaseEndpoint struct {
	Name                   string `json:"name"`
	Type                   string `json:"type"`
	PermissionTestFilename string `json:"permission_test_filename"`
}

// Validate satisfies the Model interface from model-builder. Endpoint implementations
// are built from SQL, and Validate()'d as they're ETL'd into etcd.
func (ep *BaseEndpoint) Validate() error {
	if ep.Name == "" {
		return errors.New("must specify an endpoint name")
	}
	return nil
}
