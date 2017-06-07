package cloudstore

import (
	"errors"
	"fmt"
)

// SFTPEndpoint is a fully-defined SFTP endpoint with subfolder.
type SFTPEndpoint struct {
	BaseEndpoint

	SFTPHostname  string `json:"hostname"`
	SFTPPort      string `json:"port"`
	SFTPUsername  string `json:"username"`
	SFTPPassword  string `json:"password"`
	SFTPDirectory string `json:"directory"`
	SFTPReqProxy  string `json:"req_proxy"`
	SFTPKey       string `json:"ssh_key"`
}

// Validate satisfies the model interface.
func (ep *SFTPEndpoint) Validate() error {
	if ep.SFTPPort == "" {
		return errors.New("must specify sftp port")
	} else if ep.SFTPUsername == "" {
		return errors.New("must specify sftp username")
	} else if ep.SFTPPassword == "" && ep.SFTPKey == "" {
		return errors.New("must either specify password or key")
	} else if ep.SFTPDirectory == "" {
		return errors.New("must specify sftp directory")
	}
	return ep.BaseEndpoint.Validate()
}

// CheckPermissions satisfies the Endpoint interface.
func (ep *SFTPEndpoint) CheckPermissions() error {
	return checkPermissions(ep, ep.PermissionTestFilename)
}

// Connect satisfies the Endpoint interface, returning a usable connection to the
// underlying SFTP filesystem.
func (ep *SFTPEndpoint) Connect(more Properties) (FileSystem, error) {
	var prop, err = mergeProperties(more, ep.properties())
	if err != nil {
		return nil, err
	}
	return NewFileSystem(prop, ep.uri())
}

func (ep *SFTPEndpoint) properties() Properties {
	return MapProperties{
		SFTPUsername: ep.SFTPUsername,
		SFTPPassword: ep.SFTPPassword,
		SFTPPort:     ep.SFTPPort,
		SFTPReqProxy: ep.SFTPReqProxy,
		SFTPKey:      ep.SFTPKey,
	}
}

func (ep *SFTPEndpoint) uri() string {
	return fmt.Sprintf("sftp://%s/%s", ep.SFTPHostname, ep.SFTPDirectory)
}
