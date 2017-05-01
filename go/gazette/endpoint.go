package cloudstore

import "errors"

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

// BaseEndpoint provides common fields for all endpoints. Though it currently
// only contains a |Name| field, it's important to maintain this inheritence
// to allow us to use |Name| as a primary key in the endpoint namespace.
type BaseEndpoint struct {
	Name string `json:"name"`
}

// Validate satisfies the Model interface from model-builder. Endpoint implementations
// are built from SQL, and Validate()'d as they're ETL'd into etcd.
func (ep *BaseEndpoint) Validate() error {
	if ep.Name == "" {
		return errors.New("must specify an endpoint name")
	}
	return nil
}
