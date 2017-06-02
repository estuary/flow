package cloudstore

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

// S3Endpoint is a fully-defined S3 endpoint with bucket and subfolder.
type S3Endpoint struct {
	BaseEndpoint

	AWSAccessKeyID     string `json:"access_key_id"`
	AWSSecretAccessKey string `json:"secret_access_key"`
	S3GlobalCannedACL  string `json:"global_canned_acl"`
	S3Region           string `json:"region"`
	S3Bucket           string `json:"bucket"`
	S3Subfolder        string `json:"subfolder"`
	S3SSEAlgorithm     string `json:"sse_algorithm"`
}

var (
	validSSEAlgorithms = []string{"AES256"}
)

// Validate satisfies the model interface.
func (ep *S3Endpoint) Validate() error {
	if ep.AWSSecretAccessKey == "" {
		return errors.New("must specify aws secret access key")
	} else if ep.S3Bucket == "" {
		return errors.New("must specify s3 bucket")
	} else if ep.S3SSEAlgorithm != "" && !contains(validSSEAlgorithms, ep.S3SSEAlgorithm) {
		return fmt.Errorf("no such SSE algorithm: %s", ep.S3SSEAlgorithm)
	}
	//TODO(Azim): introduce a validation for global canned acl and region
	return ep.BaseEndpoint.Validate()
}

// CheckPermissions satisfies the Endpoint interface.
func (ep *S3Endpoint) CheckPermissions() error {
	var fname = ep.PermissionTestFilename
	var fs FileSystem
	var err error

	if err = ep.Validate(); err != nil {
		return fmt.Errorf("could not validate endpoint: %s", err)
	}

	if fs, err = ep.Connect(EmptyProperties()); err != nil {
		return fmt.Errorf("could not connect: %s", err)
	}

	defer fs.Close()

	if fname != "" {
		if file, err := fs.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0640); err != nil {
			return fmt.Errorf("could not open file: %s", err)
		} else if _, err := file.Write([]byte("")); err != nil {
			return fmt.Errorf("could not write to file: %s", err)
		} else if err := file.Close(); err != nil {
			return fmt.Errorf("could not close file: %s", err)
		} else if err := fs.Remove(fname); err != nil {
			return fmt.Errorf("could not remove file: %s", err)
		}
	} else {
		// Try at least to read a file list.
		if dir, err := fs.OpenFile(".", os.O_RDONLY, 0); err != nil {
			return fmt.Errorf("could not open directory: %s", err)
		} else if _, err := dir.Readdir(0); err != nil {
			return fmt.Errorf("could not read from directory: %s", err)
		}
	}

	return nil
}

// Connect satisfies the Endpoint interface, returning a usable connection to the
// underlying S3 filesystem.
func (ep *S3Endpoint) Connect(more Properties) (FileSystem, error) {
	var prop, err = mergeProperties(more, ep.properties())
	if err != nil {
		return nil, err
	}
	return NewFileSystem(prop, ep.uri())
}

func (ep *S3Endpoint) properties() Properties {
	return MapProperties{
		AWSAccessKeyID:     ep.AWSAccessKeyID,
		AWSSecretAccessKey: ep.AWSSecretAccessKey,
		S3GlobalCannedACL:  ep.S3GlobalCannedACL,
		S3SSEAlgorithm:     ep.S3SSEAlgorithm,
		S3Region:           ep.S3Region,
	}
}

func (ep *S3Endpoint) uri() string {
	return fmt.Sprintf("s3://%s/%s", ep.S3Bucket, ep.S3Subfolder)
}

func contains(slice []string, element string) bool {
	// |sort.SearchStrings| returns the index of the occurrence or insertion
	// point if the element doesn't exist. Verify both that the returned index
	// is in-bounds, and that its value matches the search term.
	var i = sort.SearchStrings(slice, element)
	return i < len(slice) && slice[i] == element
}
