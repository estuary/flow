package cloudstore

import (
	"encoding/json"
	"os"

	log "github.com/Sirupsen/logrus"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// LocationFromEndpoint returns a URI and properties given a partner-endpoints-style path
// in etcd. Optionally, |keyPath| can be attached to SFTP authentication.
func LocationFromEndpoint(keysAPI etcd.KeysAPI, path, keyPath string) (string, Properties) {
	var endpoint struct {
		// AWS
		AWSAccessKeyID     string `json:"aws_access_key_id"`
		AWSSecretAccessKey string `json:"aws_secret_access_key"`
		S3GlobalCannedACL  string `json:"s3_global_canned_acl"`
		S3Region           string `json:"s3_region"`
		S3Bucket           string `json:"s3_bucket"`
		S3Subfolder        string `json:"s3_subfolder"`

		// TODO(joshk): Support GCS

		// SFTP
		SFTPHostname  string `json:"sftp_hostname"`
		SFTPPort      string `json:"sftp_port"`
		SFTPUsername  string `json:"sftp_username"`
		SFTPPassword  string `json:"sftp_password"`
		SFTPDirectory string `json:"sftp_directory"`
	}

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

	if endpoint.AWSAccessKeyID != "" {
		var uri = "s3://" + endpoint.S3Bucket + "/" + endpoint.S3Subfolder
		return uri, mapProperties{
			AWSAccessKeyID:     endpoint.AWSAccessKeyID,
			AWSSecretAccessKey: endpoint.AWSSecretAccessKey,
			S3GlobalCannedACL:  endpoint.S3GlobalCannedACL,
			S3Region:           endpoint.S3Region,
		}
	} else if endpoint.SFTPHostname != "" {
		var uri = "sftp://" + endpoint.SFTPHostname + "/" + endpoint.SFTPDirectory
		return uri, mapProperties{
			SFTPUsername: endpoint.SFTPUsername,
			SFTPPassword: endpoint.SFTPPassword,
			SFTPKeyPath:  keyPath,
			SFTPPort:     endpoint.SFTPPort,
		}
	}

	panic("endpoint type not supported")
}

func PropertiesFromFile(path string) Properties {
	var mp mapProperties
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

type mapProperties map[string]string

func (mp mapProperties) Get(key string) string {
	return mp[key]
}
