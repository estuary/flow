package cloudstore

import (
	"errors"
	"fmt"
	"io"
	"os"
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
	} else if ep.SFTPPassword == "" {
		return errors.New("must specify sftp password")
	} else if ep.SFTPDirectory == "" {
		return errors.New("must specify sftp directory")
	}
	return ep.BaseEndpoint.Validate()
}

// CheckPermissions satisfies the Endpoint interface.
func (ep *SFTPEndpoint) CheckPermissions() error {
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
		if file, err := fs.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640); err != nil {
			return fmt.Errorf("could not open file: %s", err)
		} else if _, err := file.Write([]byte("")); err != nil {
			return fmt.Errorf("could not write to file: %s", err)
		} else if err := file.Close(); err != nil {
			return fmt.Errorf("could not flush file: %s", err)
		} else if err := fs.Remove(fname); err != nil {
			return fmt.Errorf("could not remove file: %s", err)
		}
	} else {
		// Try at least to read a file list.
		if dir, err := fs.OpenFile("/", os.O_RDONLY, 0); err != nil {
			return fmt.Errorf("could not open directory: %s", err)
		} else if _, err := dir.Readdir(0); err != nil && err != io.EOF {
			return fmt.Errorf("could not read from directory: %s", err)
		}
	}

	return nil
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
