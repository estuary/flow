package cloudstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// Endpoint_DEPRECATED is the old-style partner-endpoint, to be replaced
// by the Endpoint interface.
type Endpoint_DEPRECATED struct {
	// AWS
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	S3GlobalCannedACL  string `json:"s3_global_canned_acl"`
	S3Region           string `json:"s3_region"`
	S3Bucket           string `json:"s3_bucket"`
	S3Subfolder        string `json:"s3_subfolder"`

	// TODO(joshk): Migrate this to 's3_sse_algorithm'.
	S3SSEAlgorithm string `json:"sse"`

	// TODO(joshk): Support GCS

	// SFTP
	SFTPHostname string `json:"sftp_hostname"`
	// TODO(joshk): This should be an integer.
	SFTPPort      string `json:"sftp_port"`
	SFTPUsername  string `json:"sftp_username"`
	SFTPPassword  string `json:"sftp_password"`
	SFTPDirectory string `json:"sftp_directory"`
}

// Validate satisfies the model interface
func (ep *Endpoint_DEPRECATED) Validate() error {
	if ep.IsSFTP() {
		if ep.SFTPPort == "" {
			return errors.New("must specify sftp port")
		} else if ep.SFTPUsername == "" {
			return errors.New("must specify sftp username")
		} else if ep.SFTPPassword == "" {
			return errors.New("must specify sftp password")
		} else if ep.SFTPDirectory == "" {
			return errors.New("must specify sftp directory")
		}
	} else if ep.IsS3() {
		if ep.AWSSecretAccessKey == "" {
			return errors.New("must specify aws secret access key")
		} else if ep.S3Bucket == "" {
			return errors.New("must specify s3 bucket")
		} else if ep.S3SSEAlgorithm != "" && !contains(validSSEAlgorithms, ep.S3SSEAlgorithm) {
			return fmt.Errorf("no such SSE algorithm: %s", ep.S3SSEAlgorithm)
		}
		//TODO: something about global canned acl and region?
	} else {
		return errors.New("can't tell what sort of Endpoint this is")
	}

	// Fall through on success.
	return nil
}

// IsS3 returns whether or not the config describes an S3 endpoint.
func (ep *Endpoint_DEPRECATED) IsS3() bool {
	return ep.AWSAccessKeyID != ""
}

// IsSFTP returns whether or not the config describes an SFTP endpoint.
func (ep *Endpoint_DEPRECATED) IsSFTP() bool {
	return ep.SFTPHostname != ""
}

// Subfolder returns the value of the directory beyond the root to upload a file to.
func (ep *Endpoint_DEPRECATED) Subfolder() string {
	if ep.IsS3() {
		return ep.S3Subfolder
	} else if ep.IsSFTP() {
		return ep.SFTPDirectory
	}
	panic("unable to determine subfolder")
}

// URI returns a fully qualified URI string for the given endpoint .
func (ep *Endpoint_DEPRECATED) URI() string {
	if ep.IsS3() {
		return "s3://" + ep.S3Bucket + "/" + ep.S3Subfolder
	} else if ep.IsSFTP() {
		return "sftp://" + ep.SFTPHostname + "/" + ep.SFTPDirectory
	}
	panic("endpoint type not supported")
}

// Properties returns a cloudstore.Properties map for the given Endpoint.
func (ep *Endpoint_DEPRECATED) Properties(keyPath string) Properties {
	if ep.IsS3() {
		return MapProperties{
			AWSAccessKeyID:     ep.AWSAccessKeyID,
			AWSSecretAccessKey: ep.AWSSecretAccessKey,
			S3GlobalCannedACL:  ep.S3GlobalCannedACL,
			S3SSEAlgorithm:     ep.S3SSEAlgorithm,
			S3Region:           ep.S3Region,
		}
	} else if ep.IsSFTP() {
		return MapProperties{
			SFTPUsername: ep.SFTPUsername,
			SFTPPassword: ep.SFTPPassword,
			SFTPKeyPath:  keyPath,
			SFTPPort:     ep.SFTPPort,
		}
	}
	panic("endpoint type not supported")
}

// LocationFromEndpoint returns a URI and properties given a partner-endpoints-style path
// in etcd. Optionally, |keyPath| can be attached to SFTP authentication.
func LocationFromEndpoint(keysAPI etcd.KeysAPI, path, keyPath string) (string, Properties) {
	var endpoint Endpoint_DEPRECATED
	var resp, err = keysAPI.Get(context.Background(), path,
		&etcd.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		log.WithFields(log.Fields{"err": err, "path": path}).Fatal(
			"failed to fetch Etcd endpoint")
	}

	if err = json.Unmarshal([]byte(resp.Node.Value), &endpoint); err != nil {
		log.WithFields(log.Fields{"err": err, "path": path}).Fatal(
			"failed to decode Etcd endpoint")
	}
	return endpoint.URI(), endpoint.Properties(keyPath)
}

func PropertiesFromFile(path string) Properties {
	var mp MapProperties
	fobj, err := os.Open(path)
	if err != nil {
		log.WithFields(log.Fields{"path": path, "err": err}).Fatal(
			"failed to open properties file")
	}
	defer fobj.Close()
	if err := json.NewDecoder(fobj).Decode(&mp); err != nil {
		log.WithFields(log.Fields{"path": path, "err": err}).Fatal(
			"failed to decode properties file")
	}
	return mp
}
